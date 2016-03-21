package org.springframework.cloud.stream.schema;

import java.util.List;

import org.apache.avro.Schema;

/**
 * @author Vinicius Carvalho
 */
public interface SchemaRegistryClient {
	/**
	 * Registers a schema with the remote repository returning the unique identifier associated with this schema version
	 * @param schema
	 * @return
	 */
	public Integer register(Schema schema);

	/**
	 * Retrieves a schema by its identifier
	 * @param id
	 * @return
	 */
	public Schema fetch(Integer id);

}
