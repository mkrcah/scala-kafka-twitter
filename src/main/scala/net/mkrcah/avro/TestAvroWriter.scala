package net.mkrcah.avro

import java.io.File

import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}


object TestAvroWriter extends App{

  val avroSchemaFile = getClass.getClassLoader.getResource("avro/tweets.avsc").toURI
  private val schema = new Schema.Parser().parse(new File(avroSchemaFile))

  val user1 = {
    val u = new Record(schema)
    u.put("name", "John")
    u.put("text", "Avro rocks!")
    u
  }

  val user2 = {
    val u = new Record(schema)
    u.put("name", "Jack")
    u.put("text", "Scala with Avro rocks!")
    u
  }


  val output = new File("tweets.avro")
  val datumWriter = new GenericDatumWriter[GenericRecord](schema)
  val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
  dataFileWriter.create(schema, output)
  dataFileWriter.append(user1)
  dataFileWriter.append(user2)
  dataFileWriter.close()

}
