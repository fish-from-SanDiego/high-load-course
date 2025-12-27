package ru.quipy.common.utils

import kotlin.math.pow
import kotlin.time.Duration
import kotlin.time.toKotlinDuration
import java.time.Duration as JDuration

fun interface RetryDelayStrategy {
    fun delayMillis(retryNumber: Int): Long
}

class FixedTimeRetryStrategy(fixedTime: Duration) : RetryDelayStrategy {
    private val fixedTimeMillis = fixedTime.inWholeMilliseconds

    constructor(fixedTime: JDuration) : this(fixedTime.toKotlinDuration())

    override fun delayMillis(retryNumber: Int): Long {
        return fixedTimeMillis
    }
}

class ExponentialBackoffDelayStrategy(
    baseDelay: Duration,
    private val factor: Double = 2.0,
    maxDelay: Duration? = null
) : RetryDelayStrategy {

    private val baseDelayMillis = baseDelay.inWholeMilliseconds
    private val maxDelayMillis = maxDelay?.inWholeMilliseconds

    constructor(
        baseDelay: JDuration,
        factor: Double = 2.0,
        maxDelay: JDuration? = null
    ) : this(
        baseDelay.toKotlinDuration(),
        factor,
        maxDelay?.toKotlinDuration()
    )

    override fun delayMillis(retryNumber: Int): Long {
        if (retryNumber <= 0) return baseDelayMillis

        val exponentialDelay = (baseDelayMillis * factor.pow(retryNumber - 1)).toLong()

        return maxDelayMillis?.let { minOf(exponentialDelay, it) } ?: exponentialDelay
    }
}