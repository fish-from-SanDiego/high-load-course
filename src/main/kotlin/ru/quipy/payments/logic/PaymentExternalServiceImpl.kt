package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.ktor.client.*
import io.ktor.client.engine.java.*
import io.ktor.client.plugins.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.network.sockets.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.CountingChannel
import ru.quipy.common.utils.ExponentialBackoffDelayStrategy
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.RetryDelayStrategy
import ru.quipy.common.utils.suspending.OngoingWindow
import ru.quipy.common.utils.suspending.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.config.PaymentAccountsConfig.AccountOptions
import ru.quipy.payments.metrics.PaymentMetricsService
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.math.min
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.DurationUnit
import kotlin.time.toKotlinDuration

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val metricsService: PaymentMetricsService,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val accountOptions: AccountOptions
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val expectedProcessingTime =
        accountOptions.expectedProcessingTime ?: requestAverageProcessingTime.toKotlinDuration()
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val expectedRps =
        min(
            rateLimitPerSec.toDouble(),
            parallelRequests / requestAverageProcessingTime.toKotlinDuration().toDouble(DurationUnit.SECONDS)
        )

    private val paymentExecutor: ThreadPoolExecutor = Executors.newFixedThreadPool(32) as ThreadPoolExecutor
    private val paymentDispatcher =
        paymentExecutor.asCoroutineDispatcher()
    private val paymentScope =
        CoroutineScope(SupervisorJob() + paymentDispatcher)

    private val eventExecutor: ThreadPoolExecutor = Executors.newFixedThreadPool(2) as ThreadPoolExecutor
    private val eventDispatcher =
        eventExecutor.asCoroutineDispatcher()


    private val eventScope =
        CoroutineScope(SupervisorJob() + eventDispatcher)

    private val queueCapacity = 50_000
    private val paymentQueue =
        CountingChannel<suspend () -> Unit>(
            Channel<suspend () -> Unit>(
                capacity = queueCapacity,
                onBufferOverflow = BufferOverflow.SUSPEND
            )
        )

    private val eventQueue =
        CountingChannel<suspend () -> Unit>(
            Channel<suspend () -> Unit>(
                capacity = queueCapacity,
                onBufferOverflow = BufferOverflow.SUSPEND
            )
        )

    @OptIn(ExperimentalCoroutinesApi::class)
    private val client = HttpClient(Java) {
        engine {
            dispatcher = Dispatchers.IO.limitedParallelism(16)
            pipelining = true
            protocolVersion = java.net.http.HttpClient.Version.HTTP_2
        }
        install(HttpTimeout) {
            requestTimeoutMillis = expectedProcessingTime.inWholeMilliseconds
        }
    }

    //    @OptIn(ExperimentalCoroutinesApi::class)
//    private val client = HttpClient(Jetty) {
//        engine {
//            dispatcher = Dispatchers.IO.limitedParallelism(16)
//            pipelining = true
//            clientCacheSize = 20
//        }
//        install(HttpTimeout) {
//            requestTimeoutMillis = expectedProcessingTime.inWholeMilliseconds
//        }
//    }

//    init {
//        client.requestPipeline.intercept(HttpRequestPipeline.Before) {
//            outgoingRateLimiter.tickSuspending()
//            proceed()
//        }
//    }


    init {
        repeat(parallelRequests) {
            paymentScope.launch {
                for (task in paymentQueue) {
                    task()
                }
            }
        }
        repeat(16) {
            eventScope.launch {
                for (task in eventQueue) {
                    task()
                }
            }
        }
    }

    private val outgoingRateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val incomingRateLimiterRate = expectedRps.toInt().coerceAtLeast(1)
    private val incomingRateLimiter = LeakingBucketRateLimiter(
        incomingRateLimiterRate.toLong(),
        Duration.ofSeconds(1),
        accountOptions.incomingRateLimiterBucketSize ?: incomingRateLimiterRate,
    )
    private val ongoingRequestsLimiter = OngoingWindow(parallelRequests)

    private val retryDelayStrategy: RetryDelayStrategy = ExponentialBackoffDelayStrategy(
        accountOptions.baseRetryDelay ?: 100.milliseconds
    )
    private val maxRequestAttempts = 5

    init {
        metricsService.registerChannelGauges(paymentQueue, "payment_queue", accountName)
        metricsService.registerChannelGauges(eventQueue, "event_queue", accountName)
        metricsService.registerExecutorGauges(paymentExecutor, "payment_executor", accountName)
        metricsService.registerExecutorGauges(eventExecutor, "payment_event_executor", accountName)
    }

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ): PaymentSubmissionResult {
        val transactionId = UUID.randomUUID()

        if (!incomingRateLimiter.tick()) {
            logTooManyRequests(transactionId, paymentId, paymentStartedAt)
            return PaymentSubmissionResult.TooManyRequests(now() + expectedProcessingTime.inWholeMilliseconds)
        }
        val offered = paymentQueue.trySend {
            performPaymentTask(paymentId, amount, paymentStartedAt, transactionId, deadline)
        }.isSuccess

        if (!offered) {
            logTooManyRequests(transactionId, paymentId, paymentStartedAt)
            return PaymentSubmissionResult.TooManyRequests(
                now() + (queueCapacity / expectedRps * 1000).toLong()
            )
        }

        return PaymentSubmissionResult.Success(paymentStartedAt)
    }

    private suspend fun performPaymentTask(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        transactionId: UUID,
        deadline: Long
    ) {
        try {
            val requestUrl =
                "http://$paymentProviderHostPort/external/process" +
                        "?serviceName=$serviceName" +
                        "&token=$token" +
                        "&accountName=$accountName" +
                        "&transactionId=$transactionId" +
                        "&paymentId=$paymentId" +
                        "&amount=$amount"

            // submission логируем ОДИН РАЗ
            eventQueue.send {
                paymentESService.update(paymentId) {
                    it.logSubmission(
                        success = true,
                        transactionId,
                        now(),
                        Duration.ofMillis(now() - paymentStartedAt)
                    )
                }
            }
            metricsService.increaseSubmittedPaymentRequestCounter("SUCCESS")

            val result = retryPayment(
                attempt = 1,
                deadline = deadline
            ) {
                ongoingRequestsLimiter.acquire()
                try {
                    if (now() + expectedProcessingTime.inWholeMilliseconds > deadline) {
                        PaymentCallResult.FinalFailure("Deadline exceeded")
                    } else {
                        metricsService.increaseSentPaymentRequestCounter(accountName)

                        val requestStart = now()
                        val callResult = executeOnce(requestUrl)

                        metricsService.requestDurationTimer(accountName)
                            .record(now() - requestStart, TimeUnit.MILLISECONDS)

                        callResult
                    }
                } finally {
                    ongoingRequestsLimiter.release()
                }
            }

            handleFinalResult(
                paymentId = paymentId,
                transactionId = transactionId,
                result = result
            )
        } catch (e: Exception) {
            logger.error(
                "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                e
            )

            eventQueue.send {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)
                }
            }

            metricsService.increaseProcessedPaymentRequestCounter(
                "FAIL - ${e.message}"
            )
        }
    }

    private tailrec suspend fun retryPayment(
        attempt: Int,
        deadline: Long,
        block: suspend () -> PaymentCallResult
    ): PaymentCallResult {
        val result = block()

        return when (result) {
            is PaymentCallResult.Success,
            is PaymentCallResult.FinalFailure -> result

            is PaymentCallResult.RetryableFailure -> {
                if (attempt >= maxRequestAttempts) {
                    PaymentCallResult.FinalFailure("Retries exhausted")
                } else {
                    val delay = retryDelayStrategy.delayMillis(attempt)
                    if (now() + delay > deadline) {
                        PaymentCallResult.FinalFailure("Deadline exceeded")
                    } else {
                        delay(delay)
                        retryPayment(attempt + 1, deadline, block)
                    }
                }
            }

            is PaymentCallResult.RetryableAfterFailure -> {
                val delay = (result.timestamp - now()).coerceAtLeast(0)
                if (now() + delay > deadline) {
                    PaymentCallResult.FinalFailure("Deadline exceeded")
                } else {
                    delay(delay)
                    retryPayment(attempt + 1, deadline, block)
                }
            }
        }
    }


    private suspend fun handleFinalResult(
        paymentId: UUID,
        transactionId: UUID,
        result: PaymentCallResult
    ) {
        when (result) {
            is PaymentCallResult.Success -> {
                logger.warn(
                    "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, " +
                            "succeeded: ${result.response.result}, message: ${result.response.message}"
                )

                eventQueue.send {
                    paymentESService.update(paymentId) {
                        it.logProcessing(
                            result.response.result,
                            now(),
                            transactionId,
                            reason = result.response.message
                        )
                    }
                }

                metricsService.increaseProcessedPaymentRequestCounter(
                    if (result.response.result) "SUCCESS"
                    else "FAIL - ${result.response.message}"
                )
            }

            is PaymentCallResult.FinalFailure -> {
                logger.error(
                    "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId, " +
                            "error: ${result.reason}"
                )

                eventQueue.send {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = result.reason)
                    }
                }

                metricsService.increaseProcessedPaymentRequestCounter(
                    "FAIL - ${result.reason}"
                )
            }

            else -> error("Unreachable")
        }
    }



    private suspend fun executeOnce(requestUrl: String): PaymentCallResult {
        return try {
            while (!outgoingRateLimiter.tick()) {
                delay((75L..250L).random())
            }
            val response = client.post(requestUrl)
            response.headers["Retry-After"]?.let {
                return try {
                    PaymentCallResult.RetryableAfterFailure(it.toLong(), "HTTP ${response.status.value}")
                } catch (_: Exception) {
                    return PaymentCallResult.FinalFailure("Invalid Retry-After header value")
                }
            }

            when (response.status.value) {
                in 500..599 -> PaymentCallResult.RetryableFailure("HTTP ${response.status.value}")
                else -> {
                    val body = try {
                        mapper.readValue(response.bodyAsText(), ExternalSysResponse::class.java)
                    } catch (_: Exception) {
                        return PaymentCallResult.FinalFailure("Invalid response body")
                    }

                    when {
                        body.result == true -> PaymentCallResult.Success(body)
                        body.message == "Temporary error" -> PaymentCallResult.RetryableFailure(body.message)
                        else -> PaymentCallResult.FinalFailure(body.message ?: "External service error")
                    }
                }
            }
        } catch (_: SocketTimeoutException) {
            PaymentCallResult.RetryableFailure("Socket timeout")
        } catch (_: HttpRequestTimeoutException) {
            PaymentCallResult.RetryableFailure("Request timeout")
        }
    }


    private suspend fun waitBeforeRetry(
        paymentId: UUID,
        transactionId: UUID,
        attempt: Int,
        delayMillis: Long,
        deadline: Long
    ): Boolean {
        if (now() + expectedProcessingTime.inWholeMilliseconds + delayMillis > deadline) {
            logger.warn("[$accountName] Not waiting retry for txId: $transactionId, payment: $paymentId; deadline would be exceeded (attempt $attempt)")
            eventQueue.send {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded.")
                }
            }
            metricsService.increaseProcessedPaymentRequestCounter("FAIL - Deadline exceeded")
            return false
        }

        if (delayMillis > 0) delay(delayMillis)
        return true
    }


    private fun logTooManyRequests(transactionId: UUID, paymentId: UUID, paymentStartedAt: Long) {
        logger.warn("[$accountName] Payment not submitted for txId: $transactionId, payment: $paymentId, reason: Too many requests")
        eventQueue.trySend {
            paymentESService.update(paymentId) {
                it.logSubmission(
                    success = false,
                    transactionId,
                    now(),
                    Duration.ofMillis(now() - paymentStartedAt)
                )
            }
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Too many requests from clients")
            }
        }


        metricsService.increaseSubmittedPaymentRequestCounter("FAIL")
        metricsService.increaseProcessedPaymentRequestCounter(
            "FAIL - Too many requests from clients"
        )
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private sealed class PaymentCallResult {
        data class Success(val response: ExternalSysResponse) : PaymentCallResult()
        data class RetryableFailure(val reason: String) : PaymentCallResult()
        data class RetryableAfterFailure(val timestamp: Long, val reason: String) : PaymentCallResult()
        data class FinalFailure(val reason: String) : PaymentCallResult()
    }

}

public fun now() = System.currentTimeMillis()