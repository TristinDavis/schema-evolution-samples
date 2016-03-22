package org.springframework.cloud.stream.codec.avro;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author Vinicius Carvalho
 */
@ConfigurationProperties(prefix = "spring.cloud.stream.codec.avro")
public class AvroCodecProperties {

	private boolean dynamicSchemaGenerationEnabled = false;
	private String readerSchema;

	public String getReaderSchema() {
		return readerSchema;
	}

	public void setReaderSchema(String readerSchema) {
		this.readerSchema = readerSchema;
	}

	public boolean isDynamicSchemaGenerationEnabled() {
		return dynamicSchemaGenerationEnabled;
	}

	public void setDynamicSchemaGenerationEnabled(boolean dynamicSchemaGenerationEnabled) {
		this.dynamicSchemaGenerationEnabled = dynamicSchemaGenerationEnabled;
	}
}
