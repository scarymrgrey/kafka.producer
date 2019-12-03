
import java.util.Properties
import org.apache.kafka.clients.producer._

object Producer {
  def main(args: Array[String]): Unit = {
    println("Start producing random requests...")
    writeToKafka("currency_requests")
  }

  def writeToKafka(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val r = scala.util.Random
    while (true) {
      val value = "{\"value\":" + r.nextInt(1000) + ",\"from_currency\":\"PLN\",\"to_currency\":\"USD\"}"
      val record = new ProducerRecord[String, String](topic, "key", value)
      producer.send(record)
      Thread.sleep(30)
    }

    producer.close()
  }
}