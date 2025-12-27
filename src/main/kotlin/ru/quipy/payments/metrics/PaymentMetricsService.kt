package ru.quipy.payments.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Timer
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CountingChannel
import java.util.concurrent.ThreadPoolExecutor

@Service
class PaymentMetricsService(
    private val metricsRegistry: PrometheusMeterRegistry
) {
    fun requestDurationTimer(accountName: String) = Timer
        .builder("http_payment_request_duration_fish_from_sd")
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

    fun <E> registerChannelGauges(
        channel: CountingChannel<E>,
        channelName: String,
        accountName: String
    ) {
        Gauge.builder(
            "${channelName}_size_fish_from_sd",
            channel
        ) { it.size().toDouble() }
            .description("Size of ${channelName} channel")
            .tag("accountName", accountName)
            .register(metricsRegistry)
    }

    fun registerExecutorGauges(
        executor: ThreadPoolExecutor,
        executorName: String,
        accountName: String
    ) {
        Gauge.builder(
            "${executorName}_active_threads_fish_from_sd",
            executor
        ) { it.activeCount.toDouble() }
            .description("Number of active threads in ${executorName}")
            .tag("accountName", accountName)
            .register(metricsRegistry)

        Gauge.builder(
            "${executorName}_threads_in_pool_fish_from_sd",
            executor
        ) { it.poolSize.toDouble() }
            .description("Number of threads in ${executorName}")
            .tag("accountName", accountName)
            .register(metricsRegistry)

        Gauge.builder(
            "${executorName}_queue_size_fish_from_sd",
            executor
        ) { it.activeCount.toDouble() }
            .description("Size of ${executorName} queue")
            .tag("accountName", accountName)
            .register(metricsRegistry)
    }
}