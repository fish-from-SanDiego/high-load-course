package ru.quipy.payments.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Configuration

@Configuration
class ClientsInfoConfig(
//    пока не использую
    @Value("\${payment.clients.expected-processing-time-millis:1000}")
    val expectedProcessingTimeMillis: Long
)
