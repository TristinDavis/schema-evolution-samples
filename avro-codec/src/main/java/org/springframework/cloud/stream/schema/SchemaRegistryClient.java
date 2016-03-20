package org.springframework.cloud.stream.schema;

import java.util.List;

import org.apache.avro.Schema;

/**
 * @author Vinicius Carvalho
 */
public interface SchemaRegistryClient {

	public Integer register(Schema schema);
	public Schema fetch(Integer id);
	public List<Schema> find(Schema schema);
	public List<Schema> find(String fqn);

}
