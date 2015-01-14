package net.mkrcah

import java.io.{ByteArrayOutputStream, File}

import net.mkrcah.Twitter.Tweet
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}


object TwitterAvroConverter {

  private val avroSchema = {
    val filename = "avro-schemas/tweets.avsc"
    val uri = getClass.getClassLoader.getResource(filename).toURI
    new Schema.Parser().parse(new File(uri))
  }

  private val writer = new GenericDatumWriter[GenericRecord](avroSchema)
  private val reader = new GenericDatumReader[GenericRecord](avroSchema)

  def tweetToAvro(t: Tweet): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)

    val r = new Record(avroSchema)
    r.put("name", t.getUser.getScreenName)
    r.put("text", t.getText)

    try {
      writer.write(r, encoder)
      encoder.flush()
      out.toByteArray
    } finally {
      out.close()
    }
  }

  def avroToStr(bytes: Array[Byte]): String = {
    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    val r = reader.read(null, decoder)
    r.toString
  }

}
