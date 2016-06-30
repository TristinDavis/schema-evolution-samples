package org.springframework.cloud.stream.schema;

import org.apache.avro.Schema;

/**
 * @author Vinicius Carvalho
 */
public class SpringSchemaRegistry implements SchemaRegistryClient {
	@Override
	public Integer register(Schema schema) {
		return null;
	}

	@Override
	public Schema fetch(Integer id) {
		return null;
	}
}
