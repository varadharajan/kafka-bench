import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.immutable.ParRange


object KafkaBench extends App {

  val conf = ConfigFactory.load

  val numThreads = conf.getInt("numThreads")
  val topic = conf.getString("topic")
  val numPartitions = conf.getInt("numPartitions")
  val bootstrapServers = conf.getStringList("brokers").asScala
  val replicationFactor = conf.getInt("replicationFactor")
  val messageSize = conf.getInt("messageSizeInKBs")
  val compressionCodec = conf.getString("compressionCodec")


  val  props = new Properties()
  props.put("bootstrap.servers", bootstrapServers.mkString(","))

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("compression.codec", compressionCodec)


  val threads: ParRange = (0 to numThreads).par
  threads.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(numThreads))

  val adminClient = AdminClient.create(props)
  adminClient.createTopics(List(new NewTopic(topic, numPartitions, replicationFactor.toShort)).asJava)

  println("Creating topics..")
  Thread.sleep(5000)

  println(
    s"""
      |Configuration:
      |Topic: $topic
      |NumPartitions: $numPartitions
      |NumThreads: $numThreads
      |Replicas: $replicationFactor
      |MessageSize: $messageSize KB
      |Compression: LZ4
    """.stripMargin)

  threads.foreach(threadId => {
    val threadIdStr = threadId.toString
    println(s"Bootstrapping Thread : $threadId")

    val data = (scala.util.Random.alphanumeric take messageSize * 1024).mkString("")

    val records = (0 to numPartitions - 1) map (partitionId => new ProducerRecord(topic, partitionId, threadIdStr, data))

    val producer = new KafkaProducer[String, String](props)

    @tailrec
    def produceData: Unit = {
      records.foreach(producer.send)
      produceData
    }

    produceData
  })
}