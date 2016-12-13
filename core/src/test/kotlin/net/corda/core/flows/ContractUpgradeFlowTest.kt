package net.corda.core.flows

import net.corda.contracts.asset.Cash
import net.corda.core.contracts.*
import net.corda.core.crypto.CompositeKey
import net.corda.core.crypto.Party
import net.corda.core.crypto.SecureHash
import net.corda.core.getOrThrow
import net.corda.core.schemas.MappedSchema
import net.corda.core.schemas.PersistentState
import net.corda.core.schemas.QueryableState
import net.corda.core.serialization.OpaqueBytes
import net.corda.core.utilities.Emoji
import net.corda.flows.*
import net.corda.node.utilities.databaseTransaction
import net.corda.schemas.CashSchema
import net.corda.testing.node.MockNetwork
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.util.*
import javax.persistence.Column
import javax.persistence.Entity
import javax.persistence.Table
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertTrue

class ContractUpgradeFlowTest {
    lateinit var mockNet: MockNetwork
    lateinit var a: MockNetwork.MockNode
    lateinit var b: MockNetwork.MockNode
    lateinit var notary: Party

    @Before
    fun setup() {
        mockNet = MockNetwork()
        val nodes = mockNet.createSomeNodes()
        a = nodes.partyNodes[0]
        b = nodes.partyNodes[1]
        notary = nodes.notaryNode.info.notaryIdentity
        mockNet.runNetwork()
    }

    @After
    fun tearDown() {
        mockNet.stopNodes()
    }

    @Test
    fun `2 parties contract upgrade`() {
        // Create dummy contract.
        val twoPartyDummyContract = DummyContract.generateInitial(0, notary, a.info.legalIdentity.ref(1), b.info.legalIdentity.ref(1))
        val stx = twoPartyDummyContract.signWith(a.services.legalIdentityKey)
                .signWith(b.services.legalIdentityKey)
                .toSignedTransaction()

        a.services.startFlow(FinalityFlow(stx, setOf(a.info.legalIdentity, b.info.legalIdentity)))
        mockNet.runNetwork()

        val atx = databaseTransaction(a.database) { a.services.storageService.validatedTransactions.getTransaction(stx.id) }
        val btx = databaseTransaction(b.database) { b.services.storageService.validatedTransactions.getTransaction(stx.id) }
        requireNotNull(atx)
        requireNotNull(btx)

        // The request is expected to be rejected because party B haven't authorise the upgrade yet.
        val rejectedFuture = a.services.startFlow(ContractUpgradeFlow.Instigator(atx!!.tx.outRef(0), DUMMY_V2_PROGRAM_ID)).resultFuture
        mockNet.runNetwork()
        assertFails { rejectedFuture.get() }

        // Party B authorise the contract state upgrade.
        b.services.vaultService.authoriseContractUpgrade(btx!!.tx.outRef<ContractState>(0), DUMMY_V2_PROGRAM_ID)

        // Party A initiate contract upgrade flow, expected to success this time.
        val resultFuture = a.services.startFlow(ContractUpgradeFlow.Instigator(atx.tx.outRef(0), DUMMY_V2_PROGRAM_ID)).resultFuture
        mockNet.runNetwork()

        val result = resultFuture.get()

        listOf(a, b).forEach {
            val stx = databaseTransaction(a.database) { a.services.storageService.validatedTransactions.getTransaction(result.ref.txhash) }
            requireNotNull(stx)

            // Verify inputs.
            assertTrue(stx!!.tx.inputs.size == 1)
            val input = databaseTransaction(a.database) { a.services.storageService.validatedTransactions.getTransaction(stx.tx.inputs.first().txhash) }
            requireNotNull(input)
            assertTrue(input!!.tx.outputs.size == 1)
            assertTrue(input.tx.outputs.first().data is DummyContract.State)

            // Verify outputs.
            assertTrue(stx.tx.outputs.size == 1)
            assertTrue(stx.tx.outputs.first().data is DummyContractV2.State)
        }
    }

    @Test
    fun `upgrade Cash to v2`() {
        // Create some cash.
        val result = a.services.startFlow(CashFlow(CashCommand.IssueCash(Amount(1000, USD), OpaqueBytes.of(1), a.info.legalIdentity, notary))).resultFuture
        mockNet.runNetwork()
        val stateAndRef = result.getOrThrow().tx.outRef<Cash.State>(0)
        // Starts contract upgrade flow.
        a.services.startFlow(ContractUpgradeFlow.Instigator(stateAndRef, CashV2()))
        mockNet.runNetwork()
        // Get contract state form the vault.
        val state = databaseTransaction(a.database) { a.vault.currentVault.states }
        assertTrue(state.size == 1)
        assertTrue(state.first().state.data is CashV2.State, "Contract state is upgraded to the new version.")
        assertEquals(Amount(1000000, USD).`issued by`(a.info.legalIdentity.ref(1)), (state.first().state.data as CashV2.State).amount, "Upgraded cash contain the correct amount.")
        assertEquals(listOf(a.info.legalIdentity.owningKey), (state.first().state.data as CashV2.State).owners, "Upgraded cash belongs to the right owner.")
    }

    class CashV2 : UpgradedContract<Cash.State, CashV2.State> {
        override val legacyContract = Cash()

        data class State(override val amount: Amount<Issued<Currency>>, val owners: List<CompositeKey>) : FungibleAsset<Currency>, QueryableState {
            override val owner: CompositeKey = owners.first()
            override val exitKeys = (owners + amount.token.issuer.party.owningKey).toSet()
            override val contract = CashV2()
            override val participants = owners

            override fun move(newAmount: Amount<Issued<Currency>>, newOwner: CompositeKey) = copy(amount = amount.copy(newAmount.quantity, amount.token), owners = listOf(newOwner))
            override fun toString() = "${Emoji.bagOfCash}New Cash($amount at ${amount.token.issuer} owned by $owner)"
            override fun withNewOwner(newOwner: CompositeKey) = Pair(Cash.Commands.Move(), copy(owners = listOf(newOwner)))

            /** Object Relational Mapping support. */
            override fun generateMappedObject(schema: MappedSchema): PersistentState {
                return when (schema) {
                    is CashSchemaV2 -> CashSchemaV2.PersistentCashState(
                            owner = this.owner.toBase58String(),
                            secondOwner = this.owners.last().toBase58String(),
                            pennies = this.amount.quantity,
                            currency = this.amount.token.product.currencyCode,
                            issuerParty = this.amount.token.issuer.party.owningKey.toBase58String(),
                            issuerRef = this.amount.token.issuer.reference.bytes
                    )
                    else -> throw IllegalArgumentException("Unrecognised schema $schema")
                }
            }

            /** Object Relational Mapping support. */
            override fun supportedSchemas(): Iterable<MappedSchema> = listOf(CashSchemaV2)
        }

        override fun upgrade(state: Cash.State) = CashV2.State(state.amount.times(1000), listOf(state.owner))

        override fun verify(tx: TransactionForContract) {}

        // Dummy Cash contract for testing.
        override val legalContractReference = SecureHash.sha256("")

    }

    object CashSchemaV2 : MappedSchema(schemaFamily = CashSchema.javaClass, version = 2, mappedTypes = listOf(PersistentCashState::class.java)) {
        @Entity
        @Table(name = "cash_states")
        class PersistentCashState(
                @Column(name = "owner_key")
                var owner: String,
                @Column(name = "second_owner_key")
                var secondOwner: String,

                @Column(name = "pennies")
                var pennies: Long,

                @Column(name = "ccy_code", length = 3)
                var currency: String,

                @Column(name = "issuer_key")
                var issuerParty: String,

                @Column(name = "issuer_ref")
                var issuerRef: ByteArray
        ) : PersistentState()
    }
}
