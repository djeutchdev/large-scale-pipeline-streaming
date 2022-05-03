import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.{LogManager,Logger}
import java.util._
import org.apache.kafka.common.security.auth.SecurityProtocol.SASL_PLAINTEXT

object ProducerKafka extends App{
  val trace_kafka = LogManager.getLogger(name = "console")
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG,"all")
  props.put("security.protocol","SASL_PLAINTEXT")

  val topic : String=""
  val message : String =""

  try{
      val twitterProducer = new KafkaProducer[String, String](props)
      val twitterRecord  = new ProducerRecord[String, String](topic, message)
      twitterProducer.send(twitterRecord)
  }
  catch
  {
    case ex: Exception => trace_kafka.error(s"erreur lors de l'envoi du message. Détails de l'erreur: ${ex.printStackTrace()}")
  }
}
