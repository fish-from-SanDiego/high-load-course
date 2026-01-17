package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    override fun submitPaymentRequest(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ): PaymentSubmissionResult {
        return paymentAccounts.fold<PaymentExternalSystemAdapter, PaymentSubmissionResult?>(null) { bestResult, account ->
            val result = account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)

            when {
                result is PaymentSubmissionResult.Success -> return result
                bestResult == null -> result
                bestResult is PaymentSubmissionResult.TooManyRequests && result is PaymentSubmissionResult.TooManyRequests ->
                    if (result.retryAfterTimestamp < bestResult.retryAfterTimestamp) result else bestResult

                else -> bestResult
            }
        } ?: throw IllegalStateException("No payment results returned from any account")
    }

}