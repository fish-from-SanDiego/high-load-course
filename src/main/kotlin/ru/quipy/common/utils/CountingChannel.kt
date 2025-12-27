package ru.quipy.common.utils

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ChannelIterator
import kotlinx.coroutines.channels.ChannelResult
import java.util.concurrent.atomic.AtomicInteger

class CountingChannel<E>(
    private val channel: Channel<E>
)
//    can't implement because of sealed interface properties
//    : Channel<E>
{
    private val counter = AtomicInteger(0)

    suspend fun send(element: E) {
        channel.send(element)
        counter.incrementAndGet()
    }

    fun trySend(element: E): ChannelResult<Unit> {
        val result = channel.trySend(element)
        if (result.isSuccess) {
            counter.incrementAndGet()
        }
        return result
    }

    operator fun iterator(): ChannelIterator<E> {
        return object : ChannelIterator<E> {
            private val channelIterator = channel.iterator()
            override suspend fun hasNext(): Boolean {
                return channelIterator.hasNext()
            }

            override fun next(): E {
                val element = channelIterator.next()
                counter.decrementAndGet()
                return element
            }

        }
    }

    fun size(): Int {
        return counter.get()
    }

}