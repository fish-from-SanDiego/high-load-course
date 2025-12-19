package ru.quipy.payments.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.springframework.stereotype.Service

@Service
class PaymentMetricsService(
    private val metricsRegistry: PrometheusMeterRegistry
) {
    fun requestLatencyTimer(accountName: String) = Timer
        .builder("http_payment_request_latency_fish_from_sd")
        .publishPercentiles(0.5, 0.75, 0.85, 0.90, 0.95, 0.99)
        .publishPercentileHistogram()
        .tags("accountName", accountName)
        .register(metricsRegistry)

    fun increasePaymentRequestRetriesCounter(accountName: String) = Counter
        .builder("http_payment_retries_fish_from_sd")
        .description("Total number of payment retries")
        .tags("accountName", accountName)
        .register(metricsRegistry)
        .increment()

    fun increaseSentPaymentRequestCounter(accountName: String) = Counter
        .builder("http_payment_requests_sent_fish_from_sd")
        .description("Total number of payment retries")
        .tags("accountName", accountName)
        .register(metricsRegistry)
        .increment()

    fun increaseReceivedPaymentRequestCounter() =
        Counter
            .builder("http_payment_requests_received_fish_from_sd")
            .description("Total number of payment http requests received from clients")
            .register(metricsRegistry)
            .increment()

    fun increaseSubmittedPaymentRequestCounter(result: String) =
        Counter
            .builder("http_payment_requests_submitted_fish_from_sd")
            .description("Total number of payment http requests submitted to payment service")
            .tags("result", result)
            .register(metricsRegistry)
            .increment()


    fun increaseProcessedPaymentRequestCounter(result: String) = Counter
        .builder("http_payment_requests_processed_fish_from_sd")
        .description("Total number of processed payment http requests")
        .tags("result", result)
        .register(metricsRegistry).increment()
}