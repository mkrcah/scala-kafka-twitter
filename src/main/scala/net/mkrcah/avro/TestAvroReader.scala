package net.mkrcah.avro

import java.io.File

import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}

import scala.collection.JavaConversions._

object TestAvroReader extends App{

  val avroSchemaFile = getClass.getClassLoader.getResource("avro/tweets.avsc").toURI
  private val schema = new Schema.Parser().parse(new File(avroSchemaFile))

  val input = new File("tweets.avro")
  val datumReader = new GenericDatumReader[GenericRecord](schema)
  val dataFileReader = new DataFileReader[GenericRecord](input, datumReader)
  for (user <- dataFileReader.iterator) {
    println(user)
  }
}
