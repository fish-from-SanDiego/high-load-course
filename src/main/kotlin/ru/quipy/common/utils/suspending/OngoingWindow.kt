package ru.quipy.common.utils.suspending

import kotlinx.coroutines.sync.Semaphore


class OngoingWindow(
    maxWinSize: Int
) {
    private val window = Semaphore(maxWinSize)

    suspend fun acquire() {
        window.acquire()
    }

    fun release() = window.release()
}