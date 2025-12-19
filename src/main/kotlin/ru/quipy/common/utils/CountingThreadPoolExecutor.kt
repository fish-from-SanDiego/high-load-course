package ru.quipy.common.utils

import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger

class CountingThreadPoolExecutor(
    corePoolSize: Int,
    maximumPoolSize: Int,
    keepAliveTime: Long,
    unit: TimeUnit,
    workQueue: BlockingQueue<Runnable>,
    threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
    handler: RejectedExecutionHandler = AbortPolicy()
) :
    ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler) {
    private val taskCount = AtomicInteger(0)

    override fun execute(command: Runnable) {
        taskCount.incrementAndGet()
        super.execute(command)
    }

    override fun afterExecute(r: Runnable?, t: Throwable?) {
        try {
            super.afterExecute(r, t)
        } finally {
            taskCount.decrementAndGet()
        }
    }

    val totalTaskCount: Int
        get() = taskCount.get()
}
