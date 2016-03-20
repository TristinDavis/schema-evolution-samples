package org.springframework.cloud.stream.codec.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.schema.SchemaRegistryClient;
import org.springframework.integration.codec.Codec;
import org.springframework.util.Assert;

/**
 * @author Vinicius Carvalho
 */
public class AvroCodec implements Codec {

	@Autowired
	private SchemaRegistryClient schemaRegistryClient;

	@Override
	public void encode(Object object, OutputStream outputStream) throws IOException {
		Schema schema = getSchema(object);
		Integer id = schemaRegistryClient.register(schema);
		DatumWriter writer = getDatumWriter(object.getClass(),schema);
		Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
		outputStream.write(ByteBuffer.allocate(4).putInt(id).array());
		writer.write(object,encoder);
		encoder.flush();
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
		Schema schema = schemaRegistryClient.fetch(schemaId);
		return null;
	}

	private DatumWriter getDatumWriter(Class<?> type, Schema schema){
		return (GenericRecord.class.isAssignableFrom(type)) ? new GenericDatumWriter<>(schema) : new SpecificDatumWriter(schema);
	}

	private DatumReader getDatumReader(Class<?> type, Schema reader, Schema writer){
		return (GenericRecord.class.isAssignableFrom(type)) ? new GenericDatumReader<>() : new SpecificDatumReader<>(type);
	}

	private Schema  getSchema(Object payload){
		if(GenericContainer.class.isAssignableFrom(payload.getClass()))
			return ((GenericContainer)payload).getSchema();
		//else find schema from local avro files in resource, use class fqn to find which schema maps
		return null;
	}
}
