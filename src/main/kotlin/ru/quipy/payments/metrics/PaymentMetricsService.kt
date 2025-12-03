package ru.quipy.payments.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.springframework.stereotype.Service

@Service
class PaymentMetricsService(
    private val metricsRegistry: PrometheusMeterRegistry
) {
    private val increaseReceivedPaymentRequestCounter = Counter
        .builder("http_payment_requests_received_fish_from_sd")
        .description("Total number of payment http requests received from clients")
        .register(metricsRegistry)

    private val increaseSubmittedPaymentRequestCounter =
        Counter
            .builder("http_payment_requests_submitted_fish_from_sd")
            .description("Total number of payment http requests submitted to payment service")
            .register(metricsRegistry)

    fun increaseReceivedPaymentRequestCounter() =
        increaseReceivedPaymentRequestCounter.increment()

    fun increaseSubmittedPaymentRequestCounter() =
        increaseSubmittedPaymentRequestCounter.increment()

    fun increaseProcessedPaymentRequestCounter(result: String) = Counter
        .builder("http_payment_requests_processed_fish_from_sd")
        .description("Total number of processed payment http requests")
        .tags("result", result)
        .register(metricsRegistry).increment()
}