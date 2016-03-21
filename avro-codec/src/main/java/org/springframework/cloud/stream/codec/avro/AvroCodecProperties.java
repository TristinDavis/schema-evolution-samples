package org.springframework.cloud.stream.codec.avro;

/**
 * @author Vinicius Carvalho
 */
public class AvroCodecProperties {
	private boolean dynamicSchemaGenerationEnabled = false;

	public boolean isDynamicSchemaGenerationEnabled() {
		return dynamicSchemaGenerationEnabled;
	}

	public void setDynamicSchemaGenerationEnabled(boolean dynamicSchemaGenerationEnabled) {
		this.dynamicSchemaGenerationEnabled = dynamicSchemaGenerationEnabled;
	}
}
