package org.springframework.cloud.stream.schema;

import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;

import org.apache.avro.Schema;

import org.springframework.context.ApplicationContext;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

/**
 * @author Vinicius Carvalho
 */
public class LocalSchemaRegistry {

	private ConcurrentHashMap<String,Schema> schemaRegistry;

	public Schema find(String fqn){
		return schemaRegistry.get(fqn);
	}


	@PostConstruct
	public void populateRegistry() throws Exception{
	}
}
