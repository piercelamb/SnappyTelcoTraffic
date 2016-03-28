package traffic

import com.datastax.spark.connector.streaming._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{StreamingContext, Milliseconds}
import org.apache.spark.sql.streaming.SnappyStreamingContext
import org.apache.spark.sql.{SaveMode, SnappyContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import play.api.libs.json.Json
import traffic.model._
import traffic.process.{Geofencer, ClusterAnalyser, MetricStatsProducer}
import traffic.util.AppConfig._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object TrafficStreamProcessor {


    def main(args: Array[String]): Unit = {

        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val sparkConf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("TrafficStreamProcessor")
            //.set("spark.cassandra.connection.host", cassandraHost)

        val sc = new SparkContext(sparkConf)
        //val snsc = SnappyStreamingContext(SnappyContext.getOrCreate(sc), Milliseconds(1000))
        val snc = SnappyContext(sc)
       // val ssc = new StreamingContext(sparkConf, batchSize)
        // ssc.checkpoint(checkpoint)

        process(snc)

sc.stop()

//        snsc.start
//        snsc.awaitTermination
    }

    def process(snsc: SnappyContext): Unit = {

        /* capture the attach stream and transform into attach events */
//        val attachStream = KafkaUtils
//            .createStream(ssc, quorum, groupId, Map(attachTopic -> 1))
//            .map(_._2)
//            .map(Json.parse)
//            .flatMap(_.asOpt[AttachEvent])

        snsc.sql("drop table if exists attachEvent")

        snsc.sql(
            "create table attachEvent(bearerid string, subscriberid long, subscriberimsi string, " +
              "subscribermsisdn string, subscriberimei string, lastname string, firstname string, address string, " +
              "city string, zip string, country string) " +
                "using column"
//              "using directkafka_stream options" +
//              "(storagelevel 'MEMORY_AND_DISK_SER_2', " +
//              "rowConverter 'traffic.util.KafkaStreamToRowsConverter', " +
//              "kafkaParams 'metadata.broker.list->localhost:9092', " +
//              " topics 'attach-topic')"
        )




        /* save the attach stream to the database */
       // attachStream.saveToCassandra(keyspace, attachTable)

//        /* capture the celltower stream and transform into celltower events */
//        val celltowerStream = KafkaUtils
//            .createStream(ssc, quorum, groupId, Map(celltowerTopic -> 1))
//            .map(_._2)
//            .map(Json.parse)
//            .flatMap(_.asOpt[CelltowerEvent])
//
//        /* join the celltower stream with the persisted attach stream (on bearerId) */
//        val unifiedStream = celltowerStream
//            .joinWithCassandraTable[AttachEvent](keyspace, attachTable)
//            .map { case (celltowerEvent, attachEvent ) =>
//                (attachEvent.subscriber, celltowerEvent.celltower, celltowerEvent.metrics)
//            }
//
//        unifiedStream.cache
//
//        MetricStatsProducer.produce(unifiedStream)
//
//        ClusterAnalyser.analyse(unifiedStream)
//
//        Geofencer.detect(unifiedStream)

    }

}

