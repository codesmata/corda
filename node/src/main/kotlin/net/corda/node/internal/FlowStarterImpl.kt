package net.corda.node.internal

import net.corda.core.concurrent.CordaFuture
import net.corda.core.context.InvocationContext
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.FlowLogicRefFactory
import net.corda.core.internal.FlowStateMachine
import net.corda.core.internal.concurrent.openFuture
import net.corda.core.internal.uncheckedCast
import net.corda.node.services.api.FlowStarter
import net.corda.node.services.messaging.DeduplicationHandler
import net.corda.node.services.statemachine.ExternalEvent
import net.corda.node.services.statemachine.StateMachineManager

class FlowStarterImpl(private val smm: StateMachineManager, private val flowLogicRefFactory: FlowLogicRefFactory) : FlowStarter {
    override fun <T> startFlow(event: ExternalEvent.ExternalStartFlowEvent<T>): CordaFuture<FlowStateMachine<T>> {
        smm.deliverExternalEvent(event)
        return event.future
    }

    override fun <T> startFlow(logic: FlowLogic<T>, context: InvocationContext): CordaFuture<FlowStateMachine<T>> {
        val startFlowEvent = object : ExternalEvent.ExternalStartFlowEvent<T>, DeduplicationHandler {
            override fun insideDatabaseTransaction() {}

            override fun afterDatabaseTransaction() {}

            override val externalCause: ExternalEvent
                get() = this
            override val deduplicationHandler: DeduplicationHandler
                get() = this

            override val flowLogic: FlowLogic<T>
                get() = logic
            override val context: InvocationContext
                get() = context

            override fun wireUpFuture(flowFuture: CordaFuture<FlowStateMachine<T>>) {
                _future.captureLater(flowFuture)
            }

            private val _future = openFuture<FlowStateMachine<T>>()
            override val future: CordaFuture<FlowStateMachine<T>>
                get() = _future

        }
        return startFlow(startFlowEvent)
    }

    override fun <T> invokeFlowAsync(
            logicType: Class<out FlowLogic<T>>,
            context: InvocationContext,
            vararg args: Any?): CordaFuture<FlowStateMachine<T>> {
        val logicRef = flowLogicRefFactory.createForRPC(logicType, *args)
        val logic: FlowLogic<T> = uncheckedCast(flowLogicRefFactory.toFlowLogic(logicRef))
        return startFlow(logic, context)
    }
}
