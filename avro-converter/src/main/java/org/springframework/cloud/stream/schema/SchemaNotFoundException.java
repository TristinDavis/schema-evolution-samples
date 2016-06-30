package org.springframework.cloud.stream.schema;

/**
 * @author Vinicius Carvalho
 */
public class SchemaNotFoundException extends RuntimeException {
	public SchemaNotFoundException(String message) {
		super(message);
	}
}
