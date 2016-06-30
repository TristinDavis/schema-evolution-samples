package org.springframework.cloud.stream.schema;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;

/**
 * @author Vinicius Carvalho
 */
public class CachingSchemaRegistryClient implements SchemaRegistryClient{

	private ConcurrentHashMap<Integer,Schema> schemaCache = new ConcurrentHashMap<>();
	private SchemaRegistryClient delegate;

	public CachingSchemaRegistryClient(SchemaRegistryClient delegate) {
		this.delegate = delegate;
	}

	@Override
	public Integer register(Schema schema) {
		Integer id = findSchema(schema);
		if(id == null){
			id = delegate.register(schema);
			schemaCache.put(id,schema);
		}
		return id;
	}

	@Override
	public Schema fetch(Integer id) {
		Schema schema = schemaCache.get(id);
		if(schema == null){
			schema = delegate.fetch(id);
			schemaCache.put(id,schema);
		}
		return schema;
	}

	private Integer findSchema(Schema schema){
		for(Map.Entry<Integer,Schema> entry : schemaCache.entrySet()){
			if(entry.getValue().equals(schema))
				return entry.getKey();
		}
		return null;
	}
}
