package net.corda.client

import net.corda.core.contracts.DOLLARS
import net.corda.core.contracts.issuedBy
import net.corda.core.flows.FlowException
import net.corda.core.getOrThrow
import net.corda.core.messaging.startFlow
import net.corda.core.node.services.ServiceInfo
import net.corda.core.random63BitValue
import net.corda.core.serialization.OpaqueBytes
import net.corda.core.toFuture
import net.corda.flows.CashCommand
import net.corda.flows.CashFlow
import net.corda.node.internal.Node
import net.corda.node.services.User
import net.corda.node.services.config.configureTestSSL
import net.corda.node.services.messaging.CordaRPCClient
import net.corda.node.services.startFlowPermission
import net.corda.node.services.transactions.ValidatingNotaryService
import net.corda.testing.node.NodeBasedTest
import org.apache.activemq.artemis.api.core.ActiveMQSecurityException
import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.Before
import org.junit.Test

class CordaRPCClientTest : NodeBasedTest() {
    private val rpcUser = User("user1", "test", permissions = setOf(startFlowPermission<CashFlow>()))
    private lateinit var node: Node
    private lateinit var client: CordaRPCClient

    @Before
    fun setUp() {
        node = startNode("Alice", rpcUsers = listOf(rpcUser), advertisedServices = setOf(ServiceInfo(ValidatingNotaryService.type))).getOrThrow()
        client = CordaRPCClient(node.configuration.artemisAddress, configureTestSSL())
    }

    @Test
    fun `log in with valid username and password`() {
        client.start(rpcUser.username, rpcUser.password)
    }

    @Test
    fun `log in with unknown user`() {
        assertThatExceptionOfType(ActiveMQSecurityException::class.java).isThrownBy {
            client.start(random63BitValue().toString(), rpcUser.password)
        }
    }

    @Test
    fun `log in with incorrect password`() {
        assertThatExceptionOfType(ActiveMQSecurityException::class.java).isThrownBy {
            client.start(rpcUser.username, random63BitValue().toString())
        }
    }

    @Test
    fun `close-send deadlock and premature shutdown on empty observable`() {
        println("Starting client")
        client.start(rpcUser.username, rpcUser.password)
        println("Creating proxy")
        val proxy = client.proxy()
        println("Starting flow")
        val flowHandle = proxy.startFlow(
                ::CashFlow,
                CashCommand.IssueCash(20.DOLLARS, OpaqueBytes.of(0), node.info.legalIdentity, node.info.legalIdentity))
        println("Started flow, waiting on result")
        flowHandle.progress.subscribe {
            println("PROGRESS $it")
        }
        println("Result: ${flowHandle.returnValue.toFuture().getOrThrow()}")
    }

    @Test
    fun `FlowException thrown by flow`() {
        client.start(rpcUser.username, rpcUser.password)
        val proxy = client.proxy()
        val handle = proxy.startFlow(::CashFlow, CashCommand.PayCash(
                amount = 100.DOLLARS.issuedBy(node.info.legalIdentity.ref(1)),
                recipient = node.info.legalIdentity
        ))
        // TODO Restrict this to CashException once RPC serialisation has been fixed
        assertThatExceptionOfType(FlowException::class.java).isThrownBy {
            handle.returnValue.toFuture().getOrThrow()
        }
    }
}
