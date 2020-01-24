package no.nav.helse.spetakkel

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

class Rapid(config: Properties,
            private val topic: String) {

    private val running = AtomicBoolean(false)

    private val consumer = KafkaConsumer(config, StringDeserializer(), StringDeserializer())
    private val listeners = mutableListOf<MessageListener>()

    fun register(listener: MessageListener) {
        listeners.add(listener)
    }

    fun isRunning() = running.get()

    fun start() {
        running.set(true)
        consumer.use { consumeMessages() }
    }

    fun stop() {
        running.set(false)
        consumer.wakeup()
    }

    private fun consumeMessages() {
        try {
            consumer.subscribe(listOf(topic))
            while (running.get()) {
                val records = consumer.poll(Duration.ofSeconds(1))
                records.forEach { record ->
                    listeners.forEach { it.onMessage(record.value()) }
                }
            }
        } catch (err: WakeupException) {
            // Ignore exception if closing
            if (running.get()) throw err
        }
    }

    interface MessageListener {
        fun onMessage(message: String)
    }
}
