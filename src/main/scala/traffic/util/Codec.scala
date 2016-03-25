package traffic.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.streaming.StreamToRowsConverter
import org.apache.spark.unsafe.types.UTF8String
import traffic.model.AttachEvent

/**
  * Created by plamb on 3/25/16.
  */


class KafkaStreamToRowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[InternalRow] = {
      val log = message.asInstanceOf[AttachEvent]
    Seq(InternalRow.fromSeq(
      Seq(
        UTF8String.fromString(log.bearerId),
        log.subscriber.id,
        UTF8String.fromString(log.subscriber.imsi),
        UTF8String.fromString(log.subscriber.msisdn),
        UTF8String.fromString(log.subscriber.imei),
        UTF8String.fromString(log.subscriber.lastName),
        UTF8String.fromString(log.subscriber.firstName),
        UTF8String.fromString(log.subscriber.address),
        UTF8String.fromString(log.subscriber.city),
        UTF8String.fromString(log.subscriber.zip),
        UTF8String.fromString(log.subscriber.country),
        UTF8String.fromString(log.topic)
      )
    ))
  }
}
