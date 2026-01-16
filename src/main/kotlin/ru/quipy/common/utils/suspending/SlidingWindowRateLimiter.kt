package ru.quipy.common.utils.suspending

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.RateLimiter
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicLong

class SlidingWindowRateLimiter(
    private val rate: Int,
    private val window: Duration,
) : RateLimiter {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    private val channel = Channel<Unit>(capacity = rate)

    override fun tick(): Boolean =
        channel.trySend(Unit).isSuccess

    suspend fun tickSuspending() {
        channel.send(Unit)
    }

    init {
        scope.launch {
            for (item in channel) {
                launch {
                    delay(window.toMillis())
                    try {
                        channel.receive()
                    } catch (th: Throwable) {
                        logger.error("Rate limiter release failed", th)
                    }
                }
            }
        }
    }

    companion object {
        private val logger: Logger =
            LoggerFactory.getLogger(SlidingWindowRateLimiter::class.java)
    }
}