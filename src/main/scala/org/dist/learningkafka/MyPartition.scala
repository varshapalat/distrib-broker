package org.dist.learningkafka

import java.io._

import org.dist.simplekafka.common.{Logging, TopicAndPartition}
import org.dist.simplekafka.server.Config
import org.dist.simplekafka.{FetchIsolation, FetchLogEnd, SequenceFile}

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class MyPartition(config: Config, topicAndPartition: TopicAndPartition) extends Logging {
  val LogFileSuffix = ".log"
  val logFile =
    new File(config.logDirs(0), topicAndPartition.topic + "-" + topicAndPartition.partition + LogFileSuffix)
  val sequenceFile = new SequenceFile()
  val writer = new sequenceFile.Writer(logFile.getAbsolutePath)
  val reader = new sequenceFile.Reader(logFile.getAbsolutePath)


  def append(key: String, message: String) = {
    val currentPos = writer.getCurrentPosition
    try writer.append(key, message)
    catch {
      case e: IOException =>
        writer.seek(currentPos)
        throw e
    }
  }

  def read(offset: Long = 0, replicaId: Int = -1, isolation: FetchIsolation = FetchLogEnd): List[Row] = {
    val result = new java.util.ArrayList[Row]()
    val offsets = sequenceFile.getAllOffSetsFrom(offset)
    offsets.foreach(offset ⇒ {
      val filePosition = sequenceFile.offsetIndexes.get(offset)

      val ba = new ByteArrayOutputStream()
      val baos = new DataOutputStream(ba)

      reader.seekToOffset(filePosition)
      reader.next(baos)

      val bais = new DataInputStream(new ByteArrayInputStream(ba.toByteArray))
      Try(Row.deserialize(bais)) match {
        case Success(row) => result.add(row)
        case Failure(exception) => None
      }
    })
    result.asScala.toList
  }

  object Row {
    def serialize(row: Row, dos: DataOutputStream): Unit = {
      dos.writeUTF(row.key)
      dos.writeInt(row.value.getBytes().size)
      dos.write(row.value.getBytes) //TODO: as of now only supporting string writes.
    }

    def deserialize(dis: DataInputStream): Row = {
      val key = dis.readUTF()
      val dataSize = dis.readInt()
      val bytes = new Array[Byte](dataSize)
      dis.read(bytes)
      val value = new String(bytes) //TODO:As of now supporting only string values
      Row(key, value)
    }
  }

  case class Row(key: String, value: String)
}

