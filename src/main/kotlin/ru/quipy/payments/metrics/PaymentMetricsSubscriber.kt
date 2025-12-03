package ru.quipy.payments.metrics

import jakarta.annotation.PostConstruct
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.OnlineShopApplication.Companion.appExecutor
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.api.PaymentProcessedEvent
import ru.quipy.payments.api.PaymentSubmittedEvent
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy

@Service
class PaymentMetricsSubscriber {

    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @Autowired
    lateinit var metricsService: PaymentMetricsService

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(
            PaymentAggregate::class,
            "metrics:payment-subscriber",
            retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)
        ) {
            `when`(PaymentProcessedEvent::class) { event ->
                appExecutor.submit {
                    metricsService.increaseProcessedPaymentRequestCounter(
                        if (event.success)
                            "SUCESS"
                        else
                            "FAIL" + (event.reason?.let { " - $it" } ?: "")
                    )
                }
            }
            `when`(PaymentSubmittedEvent::class) { event ->
                appExecutor.submit {
                    metricsService.increaseSubmittedPaymentRequestCounter(if (event.success) "SUCCESS" else "FAIL")
                }
            }
        }
    }
}