package org.springframework.cloud.stream.schema;

import java.io.ByteArrayOutputStream;

import io.igx.android.Sensor;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * @author Vinicius Carvalho
 */
public class SpecificAvroSerializer implements Serializer<Sensor> {

	private SpecificDatumWriter<Sensor> writer = new SpecificDatumWriter<>(Sensor.class);
	private SpecificDatumReader<Sensor> reader = new SpecificDatumReader<>(Sensor.SCHEMA$);
	private BinaryEncoder encoder;
	private BinaryDecoder decoder;

	@Override
	public Sensor read(byte[] bytes) throws Exception{
		decoder = DecoderFactory.get().binaryDecoder(bytes,this.decoder);
		return reader.read(null,decoder);
	}

	@Override
	public byte[] write(Sensor type) throws Exception{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		encoder = EncoderFactory.get().binaryEncoder(baos,this.encoder);
		writer.write(type,encoder);
		encoder.flush();
		return baos.toByteArray();
	}
}
