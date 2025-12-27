package ru.quipy.common.utils

class ConjunctiveCompositeRateLimiter(
    private val rl1: RateLimiter,
    private val rl2: RateLimiter,
) : RateLimiter {
    override fun tick(): Boolean {
        return rl1.tick() && rl2.tick()
    }
}

class DisjunctiveCompositeRateLimiter(
    private val rl1: RateLimiter,
    private val rl2: RateLimiter,
) : RateLimiter {
    override fun tick(): Boolean {
        return rl1.tick() || rl2.tick()
    }
}