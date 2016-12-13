package net.corda.flows

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.contracts.*
import net.corda.core.crypto.CompositeKey
import net.corda.core.crypto.Party
import net.corda.core.transactions.SignedTransaction
import net.corda.core.transactions.TransactionBuilder
import net.corda.flows.AbstractStateReplacementFlow.Proposal
import net.corda.flows.ContractUpgradeFlow.Acceptor
import net.corda.flows.ContractUpgradeFlow.Instigator

/**
 * A flow to be used for upgrading state objects of an old contract to a new contract.
 *
 * The [Instigator] assembles the transaction for contract replacement and sends out change proposals to all participants
 * ([Acceptor]) of that state. If participants agree to the proposed change, they each sign the transaction.
 * Finally, [Instigator] sends the transaction containing all signatures back to each participant so they can record it and
 * use the new updated state for future transactions.
 */
object ContractUpgradeFlow {
    private fun <OldState : ContractState, NewState : ContractState> assembleBareTx(stateRef: StateAndRef<OldState>,
                                                                                    contractUpgrade: UpgradedContract<OldState, NewState>): TransactionBuilder {
        return TransactionType.General.Builder(stateRef.state.notary).apply {
            withItems(stateRef, contractUpgrade.upgrade(stateRef.state.data), Command(UpgradeCommand(contractUpgrade), stateRef.state.data.participants))
        }
    }

    class Instigator<OldState : ContractState, NewState : ContractState>(
            originalState: StateAndRef<OldState>,
            newContract: UpgradedContract<OldState, NewState>) : AbstractStateReplacementFlow.Instigator<OldState, NewState, UpgradedContract<OldState, NewState>>(originalState, newContract) {

        override fun assembleTx(): Pair<SignedTransaction, Iterable<CompositeKey>> {
            return assembleBareTx(originalState, modification).let {
                it.signWith(serviceHub.legalIdentityKey)
                Pair(it.toSignedTransaction(false), originalState.state.data.participants)
            }
        }
    }

    class Acceptor(otherSide: Party) : AbstractStateReplacementFlow.Acceptor<UpgradedContract<ContractState, ContractState>>(otherSide) {
        @Suspendable
        @Throws(StateReplacementException::class)
        override fun verifyProposal(proposal: Proposal<UpgradedContract<ContractState, ContractState>>) {
            val stx = serviceHub.storageService.validatedTransactions.getTransaction(proposal.stateRef.txhash) ?: throw IllegalStateException("We don't have a copy of the referenced state")
            val state = stx.tx.outRef<ContractState>(proposal.stateRef.index)
            val authorisedUpgrade = serviceHub.vaultService.getAuthorisedContractUpgrade(state) ?: throw IllegalStateException("Contract state upgrade is unauthorised. State hash : ${state.ref}")
            val actualTx = proposal.stx.tx
            val expectedTx = assembleBareTx(state, proposal.modification).toWireTransaction()
            requireThat {
                "the instigator is one of the participants" by (state.state.data.participants.contains(otherSide.owningKey))
                "the proposed upgrade ${proposal.modification} is a trusted upgrade path" by (proposal.modification.javaClass == authorisedUpgrade.javaClass)
                "the proposed tx matches the expected tx for this upgrade" by (actualTx == expectedTx)
                "number of inputs and outputs match" by (proposal.stx.tx.inputs.size == proposal.stx.tx.outputs.size)
                "input belongs to the legacy contract" by (state.state.data.contract.javaClass == proposal.modification.legacyContract.javaClass)
                "output belongs to the upgraded contract" by proposal.stx.tx.outputs.all { it.data.contract.javaClass == proposal.modification.javaClass }
            }
        }
    }
}
