package no.nav.helse.rapids_rivers

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

class Rapid(consumerConfig: Properties,
            producerConfig: Properties,
            private val topic: String) {

    private val log = LoggerFactory.getLogger(Rapid::class.java)

    private val running = AtomicBoolean(false)

    private val stringDeserializer = StringDeserializer()
    private val stringSerializer = StringSerializer()
    private val consumer = KafkaConsumer(consumerConfig, stringDeserializer, stringDeserializer)
    private val producer = KafkaProducer(producerConfig, stringSerializer, stringSerializer)

    private val listeners = mutableListOf<MessageListener>()

    fun register(listener: MessageListener) {
        listeners.add(listener)
    }

    fun isRunning() = running.get()

    fun start() {
        log.info("starting rapid")
        running.set(true)
        consumer.use { consumeMessages() }
        stop()
    }

    fun stop() {
        log.info("stopping rapid")
        if (!running.get()) return log.info("rappid already stopped")
        running.set(false)
        producer.close()
        consumer.wakeup()
    }

    private fun consumeMessages() {
        try {
            consumer.subscribe(listOf(topic))
            while (running.get()) {
                val records = consumer.poll(Duration.ofSeconds(1))
                records.forEach { record ->
                    val context = MessageContext(record, producer)
                    listeners.forEach { it.onMessage(record.value(), context) }
                }
            }
        } catch (err: WakeupException) {
            // Ignore exception if closing
            if (running.get()) throw err
        } finally {
            log.info("stopped consuming messages")
            consumer.unsubscribe()
        }
    }

    class MessageContext(private val record: ConsumerRecord<String, String>,
                         private val producer: Producer<String, String>) {
        fun send(message: String) {
            send(record.key(), message)
        }

        fun send(key: String, message: String) {
            producer.send(ProducerRecord(record.topic(), key, message))
        }
    }

    interface MessageListener {
        fun onMessage(message: String, context: MessageContext)
    }
}
