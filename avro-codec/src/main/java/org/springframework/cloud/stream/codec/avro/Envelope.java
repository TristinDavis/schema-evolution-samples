package org.springframework.cloud.stream.codec.avro;

/**
 * @author Vinicius Carvalho
 */
public class Envelope<T> {

	private Integer schemaId;
	private T payload;

	public Integer getSchemaId() {
		return schemaId;
	}

	public void setSchemaId(Integer schemaId) {
		this.schemaId = schemaId;
	}

	public T getPayload() {
		return payload;
	}

	public void setPayload(T payload) {
		this.payload = payload;
	}
}
