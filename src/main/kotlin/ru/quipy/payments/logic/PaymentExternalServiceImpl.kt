package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.OnlineShopApplication.Companion.appExecutor
import ru.quipy.common.utils.*
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.config.PaymentAccountsConfig.AccountOptions
import ru.quipy.payments.metrics.PaymentMetricsService
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.math.min
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

        val emptyBody = RequestBody.create(null, ByteArray(0))
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

    private val paymentExecutor = ThreadPoolExecutor(
        parallelRequests,
        parallelRequests,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(8_000),
        NamedThreadFactory("payment-external-executor-${accountName}"),
        ThreadPoolExecutor.AbortPolicy()
    )

    private val httpConnectionPool = ConnectionPool(
        parallelRequests,
        maxOf(
            expectedProcessingTime.inWholeMilliseconds * 5,
            Duration.ofSeconds(30).toMillis()
        ),
        TimeUnit.MILLISECONDS
    )

    private val client = OkHttpClient.Builder()
        .connectionPool(httpConnectionPool)
        .callTimeout(expectedProcessingTime.inWholeMilliseconds, TimeUnit.MILLISECONDS)
        .build()

    private val outgoingRateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val incomingRateLimiterRate = expectedRps.toInt().coerceAtLeast(1)
    private val incomingRateLimiter = LeakingBucketRateLimiter(
        incomingRateLimiterRate.toLong(),
        Duration.ofSeconds(1),
        accountOptions.incomingRateLimiterBucketSize ?: incomingRateLimiterRate,
    )
    private val ongoingRequestsLimiter = OngoingWindow(parallelRequests, fair = false)

    private val retryDelayStrategy: RetryDelayStrategy = ExponentialBackoffDelayStrategy(
        expectedProcessingTime / 2
    )
    private val maxRequestAttempts = 5

    init {
        metricsService.registerPaymentExecutorGauges(paymentExecutor, accountName)
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

        try {
            paymentExecutor.submit { performPaymentTask(paymentId, amount, paymentStartedAt, transactionId, deadline) }
        } catch (_: RejectedExecutionException) {
            logTooManyRequests(transactionId, paymentId, paymentStartedAt)
            return PaymentSubmissionResult.TooManyRequests(
                now() + (paymentExecutor.queue.size.toDouble() / expectedRps * 1000).toLong()
            )
        }
        return PaymentSubmissionResult.Success(paymentStartedAt)
    }

    private fun performPaymentTask(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        transactionId: UUID,
        deadline: Long
    ) {
        try {
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            val originalCall = client.newCall(request)

            for (attempt in 1..maxRequestAttempts) {
                ongoingRequestsLimiter.acquire()
                val callResult = try {
                    if (now() + expectedProcessingTime.inWholeMilliseconds > deadline) {
                        logger.warn("[$accountName] Not attempting request for txId: $transactionId, payment: $paymentId; deadline would be exceeded (attempt $attempt)")
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded.")
                        }
                        appExecutor.submit {
                            metricsService.increaseProcessedPaymentRequestCounter("FAIL - Deadline exceeded")
                        }
                        return
                    }

                    outgoingRateLimiter.tickBlocking()

                    appExecutor.submit {
                        metricsService.increaseSentPaymentRequestCounter(accountName)
                        if (attempt != 1) {
                            metricsService.increasePaymentRequestRetriesCounter(accountName)
                        }
                    }

                    if (attempt == 1) {
                        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

                        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
                        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
                        paymentESService.update(paymentId) {
                            it.logSubmission(
                                success = true,
                                transactionId,
                                now(),
                                Duration.ofMillis(now() - paymentStartedAt)
                            )
                        }
                        appExecutor.submit {
                            metricsService.increaseSubmittedPaymentRequestCounter("SUCCESS")
                        }

                        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")
                    }

//                Supplier<T> может вернуть T?, но executeOnce не возвращает null
                    metricsService.requestLatencyTimer(accountName)
                        .record<PaymentCallResult> { executeOnce(originalCall.clone()) }!!
                } finally {
                    ongoingRequestsLimiter.release()
                }
                when (callResult) {
                    is PaymentCallResult.Success -> {
                        val body = callResult.response
                        logger.warn(
                            "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, " +
                                    "succeeded: ${body.result}, message: ${body.message} (attempt $attempt)"
                        )
                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                        appExecutor.submit {
                            metricsService.increaseProcessedPaymentRequestCounter(
                                if (body.result == false)
                                    "FAIL - ${body.message}"
                                else
                                    "SUCCESS"
                            )
                        }
                        return
                    }

                    is PaymentCallResult.RetryableAfterFailure -> {
                        val delay = (callResult.timestamp - now()).coerceAtLeast(0L)
                        logger.warn(
                            "[$accountName] Payment failed for txId: $transactionId, " +
                                    "payment: $paymentId, error: ${callResult.reason} (attempt $attempt); retrying"
                        )
                        if (attempt < maxRequestAttempts) {
                            if (!waitBeforeRetry(paymentId, transactionId, attempt, delay, deadline)) return
                        }
                    }

                    is PaymentCallResult.RetryableFailure -> {
                        val delay = retryDelayStrategy.delayMillis(attempt)
                        logger.warn(
                            "[$accountName] Payment failed for txId: $transactionId, " +
                                    "payment: $paymentId, error: ${callResult.reason} (attempt $attempt); retrying"
                        )
                        if (attempt < maxRequestAttempts) {
                            if (!waitBeforeRetry(paymentId, transactionId, attempt, delay, deadline)) return
                        }
                    }

                    is PaymentCallResult.FinalFailure -> {
                        logger.error(
                            "[$accountName] Payment failed for txId: $transactionId, " +
                                    "payment: $paymentId, error: ${callResult.reason} (attempt $attempt)"
                        )
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = callResult.reason)
                        }
                        appExecutor.submit {
                            metricsService.increaseProcessedPaymentRequestCounter(
                                "FAIL - ${callResult.reason}"
                            )
                        }
                        return
                    }
                }
            }

            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId - retries exhausted")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Retries exhausted")
            }
            appExecutor.submit {
                metricsService.increaseProcessedPaymentRequestCounter("FAIL - Retries exhausted")
            }
        } catch (e: Exception) {
            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = e.message)
            }
            appExecutor.submit {
                metricsService.increaseProcessedPaymentRequestCounter(
                    "FAIL - ${e.message}"
                )
            }
        }
    }

    private fun executeOnce(requestCall: Call): PaymentCallResult {
        try {
            requestCall.execute().use { response ->
                response.header("Retry-After")?.let {
                    try {
                        return PaymentCallResult.RetryableAfterFailure(it.toLong(), "HTTP ${response.code}")
                    } catch (_: Exception) {
                        return PaymentCallResult.FinalFailure("Invalid Retry-After header value")
                    }
                }

                if (response.code in 500..599) {
                    return PaymentCallResult.RetryableFailure("HTTP ${response.code}")
                }

                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (_: Exception) {
                    return PaymentCallResult.FinalFailure("Invalid response body")
                }

                return when {
                    body.result == true ->
                        PaymentCallResult.Success(body)

                    body.message == "Temporary error" ->
                        PaymentCallResult.RetryableFailure(body.message)

                    else ->
                        PaymentCallResult.FinalFailure(body.message ?: "External service error")
                }
            }
        } catch (_: SocketTimeoutException) {
            return PaymentCallResult.RetryableFailure("Socket timeout")
        } catch (_: InterruptedIOException) {
            return PaymentCallResult.RetryableFailure("Request timeout")
        }
    }


    private fun waitBeforeRetry(
        paymentId: UUID,
        transactionId: UUID,
        attempt: Int,
        delayMillis: Long,
        deadline: Long
    ): Boolean {
        if (now() + expectedProcessingTime.inWholeMilliseconds + delayMillis > deadline) {
            logger.warn("[$accountName] Not waiting retry for txId: $transactionId, payment: $paymentId; deadline would be exceeded (attempt $attempt)")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded.")
            }
            appExecutor.submit {
                metricsService.increaseProcessedPaymentRequestCounter("FAIL - Deadline exceeded")
            }
            return false
        }

        if (delayMillis > 0) Thread.sleep(delayMillis)
        return true
    }


    private fun logTooManyRequests(transactionId: UUID, paymentId: UUID, paymentStartedAt: Long) {
        logger.warn("[$accountName] Payment not submitted for txId: $transactionId, payment: $paymentId, reason: Too many requests")
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
        appExecutor.submit {
            metricsService.increaseSubmittedPaymentRequestCounter("FAIL")
            metricsService.increaseProcessedPaymentRequestCounter(
                "FAIL - Too many requests from clients"
            )
        }
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