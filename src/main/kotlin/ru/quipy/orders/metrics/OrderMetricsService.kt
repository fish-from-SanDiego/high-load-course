package ru.quipy.orders.metrics

import io.micrometer.core.instrument.Timer
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.springframework.stereotype.Service

@Service
class OrderMetricsService(
    private val metricsRegistry: PrometheusMeterRegistry
) {
    val orderSaveDurationTimer = Timer
        .builder("order_save_duration_fish_from_sd")
        .publishPercentiles(0.5, 0.75, 0.85, 0.90, 0.95, 0.99)
        .publishPercentileHistogram()
        .register(metricsRegistry)

    val orderFindDurationTimer = Timer
        .builder("order_find_duration_fish_from_sd")
        .publishPercentiles(0.5, 0.75, 0.85, 0.90, 0.95, 0.99)
        .publishPercentileHistogram()
        .register(metricsRegistry)
}