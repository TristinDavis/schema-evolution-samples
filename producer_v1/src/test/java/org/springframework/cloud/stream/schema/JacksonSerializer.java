package org.springframework.cloud.stream.schema;

import java.io.ByteArrayOutputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.igx.android.JsonSensor;

/**
 * @author Vinicius Carvalho
 */
public class JacksonSerializer implements Serializer<JsonSensor> {

	private ObjectMapper mapper = new ObjectMapper();

	@Override
	public JsonSensor read(byte[] bytes) throws Exception {
		return mapper.readValue(bytes,JsonSensor.class);
	}

	@Override
	public byte[] write(JsonSensor type) throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		mapper.writeValue(baos,type);

		return baos.toByteArray();
	}
}
