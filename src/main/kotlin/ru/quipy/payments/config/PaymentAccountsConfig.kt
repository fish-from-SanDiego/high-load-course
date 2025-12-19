package ru.quipy.payments.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.PaymentAccountProperties
import ru.quipy.payments.logic.PaymentAggregateState
import ru.quipy.payments.logic.PaymentExternalSystemAdapter
import ru.quipy.payments.logic.PaymentExternalSystemAdapterImpl
import ru.quipy.payments.metrics.PaymentMetricsService
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.*
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds


@Configuration
class PaymentAccountsConfig {
    companion object {
        private val javaClient = HttpClient.newBuilder().build()
        private val mapper = ObjectMapper().registerKotlinModule().registerModules(JavaTimeModule())
    }

    @Autowired
    lateinit var metricsService: PaymentMetricsService

    @Value("\${payment.hostPort}")
    lateinit var paymentProviderHostPort: String

    @Value("\${payment.service-name}")
    lateinit var serviceName: String

    @Value("\${payment.token}")
    lateinit var token: String

    @Value("#{'\${payment.accounts}'.split(',')}")
    lateinit var allowedAccounts: List<String>

    @Value("#{\${payment.account-leaking-bucket-size-map}}")
    val leakingBucketSizeByAccount: Map<String, Int> = mapOf()

    @Value("#{\${payment.account-expected-processing-time-map}}")
    val expectedProcessingTimeMillisByAccount: Map<String, Int> = mapOf()

    @Bean
    fun accountAdapters(paymentService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>): List<PaymentExternalSystemAdapter> {
        val request = HttpRequest.newBuilder()
            .uri(URI("http://${paymentProviderHostPort}/external/accounts?serviceName=$serviceName&token=$token"))
            .GET()
            .build()

        val resp = javaClient.send(request, HttpResponse.BodyHandlers.ofString())

        println("\nPayment accounts list:")
        return mapper.readValue<List<PaymentAccountProperties>>(
            resp.body(),
            mapper.typeFactory.constructCollectionType(List::class.java, PaymentAccountProperties::class.java)
        )
            .filter { it.accountName in allowedAccounts }
            .map { it.copy(enabled = true) }
            .onEach(::println)
            .map {
                PaymentExternalSystemAdapterImpl(
                    it,
                    paymentService,
                    metricsService,
                    paymentProviderHostPort,
                    token,
                    AccountOptions(
                        leakingBucketSizeByAccount.get(it.accountName),
                        expectedProcessingTimeMillisByAccount.get(it.accountName)?.milliseconds
                    )
                )
            }
    }

    public data class AccountOptions(
        val incomingRateLimiterBucketSize: Int?,
        val expectedProcessingTime: Duration?
    )
}