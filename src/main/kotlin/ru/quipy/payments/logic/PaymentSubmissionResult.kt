package ru.quipy.payments.logic

sealed class PaymentSubmissionResult {
    data class TooManyRequests(val retryAfterTimestamp: Long) : PaymentSubmissionResult()
    data class Success(val paymentStartedAt: Long) : PaymentSubmissionResult()
}
