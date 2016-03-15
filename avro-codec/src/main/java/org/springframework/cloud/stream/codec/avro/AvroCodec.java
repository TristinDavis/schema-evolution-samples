package org.springframework.cloud.stream.codec.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.compress.utils.IOUtils;

import org.springframework.integration.codec.Codec;
import org.springframework.util.Assert;

/**
 * @author Vinicius Carvalho
 */
public class AvroCodec implements Codec {
	@Override
	public void encode(Object object, OutputStream outputStream) throws IOException {
		DatumWriter writer = getDatumWriter(object.getClass());
		Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);


	}

	@Override
	public byte[] encode(Object o) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		encode(o,baos);
		return baos.toByteArray();
	}

	@Override
	public <T> T decode(InputStream inputStream, Class<T> type) throws IOException {
		return decode(IOUtils.toByteArray(inputStream),type);
	}

	@Override
	public <T> T decode(byte[] bytes, Class<T> type) throws IOException {
		Assert.notNull(bytes, "'bytes' cannot be null");
		Assert.notNull(bytes, "Class can not be null");
		ByteBuffer buf = ByteBuffer.wrap(bytes);
		byte[] payload = new byte[bytes.length-4];
		Integer schemaId = buf.getInt();
		Schema schema = getSchema(schemaId);
		return null;
	}

	private DatumWriter getDatumWriter(Class<?> type){
		return (GenericRecord.class.isAssignableFrom(type)) ? new GenericDatumWriter<>() : new SpecificDatumWriter(type);
	}

	private DatumReader getDatumReader(Class<?> type){
		return (GenericRecord.class.isAssignableFrom(type)) ? new GenericDatumReader<>() : new SpecificDatumReader<>(type);
	}

	private Schema  getSchema(int id){
		return null;
	}
}
