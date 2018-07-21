package net.corda.node.services.api

import net.corda.core.concurrent.CordaFuture
import net.corda.core.context.InvocationContext
import net.corda.core.flows.FlowLogic
import net.corda.core.internal.FlowStateMachine
import net.corda.node.services.statemachine.ExternalEvent

interface FlowStarter {

    /**
     * Starts an already constructed flow. Note that you must be on the server thread to call this method. This method
     * just synthesizes an [ExternalEvent.ExternalStartFlowEvent] and calls the method below.
     * @param context indicates who started the flow, see: [InvocationContext].
     */
    fun <T> startFlow(logic: FlowLogic<T>, context: InvocationContext): CordaFuture<FlowStateMachine<T>>

    /**
     * Starts a flow as described by an [ExternalEvent.ExternalStartFlowEvent].  If a transient error
     * occurs during invocation, it will re-attempt to start the flow.
     */
    fun <T> startFlow(event: ExternalEvent.ExternalStartFlowEvent<T>): CordaFuture<FlowStateMachine<T>>

    /**
     * Will check [logicType] and [args] against a whitelist and if acceptable then construct and initiate the flow.
     * Note that you must be on the server thread to call this method. [context] points how flow was started,
     * See: [InvocationContext].
     *
     * @throws net.corda.core.flows.IllegalFlowLogicException or IllegalArgumentException if there are problems with the
     * [logicType] or [args].
     */
    fun <T> invokeFlowAsync(
            logicType: Class<out FlowLogic<T>>,
            context: InvocationContext,
            vararg args: Any?): CordaFuture<FlowStateMachine<T>>
}