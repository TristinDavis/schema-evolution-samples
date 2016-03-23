package org.springframework.cloud.stream.codec.avro;

import org.apache.avro.io.DatumWriter;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.schema.CachingSchemaRegistryClient;
import org.springframework.cloud.stream.schema.ConfluentSchemaRegistryClient;
import org.springframework.cloud.stream.schema.SchemaRegistryClient;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.integration.codec.Codec;

/**
 * @author Vinicius Carvalho
 */
@Configuration
@EnableConfigurationProperties({AvroCodecProperties.class})
@ConditionalOnClass(DatumWriter.class)
public class AvroCodecAutoConfiguration {

	@Bean
	public Codec avroCodec(AvroCodecProperties properties, SchemaRegistryClient schemaRegistryClient, ApplicationContext ctx) throws Exception{
		AvroCodec codec = new AvroCodec();
		codec.setProperties(properties);
		codec.setSchemaRegistryClient(schemaRegistryClient);
		codec.setResolver(ctx);
		codec.init();
		return codec;
	}

	@Bean
	@ConditionalOnProperty("confluent.schemaregistry.endpoint")
	public SchemaRegistryClient confluentClient(@Value("${confluent.schemaregistry.endpoint}") String endpoint){
		ConfluentSchemaRegistryClient registryClient = new ConfluentSchemaRegistryClient(endpoint);
		return registryClient;
	}

	@Bean
	@Primary
	public SchemaRegistryClient schemaRegistryClient(SchemaRegistryClient client){
		return new CachingSchemaRegistryClient(client);
	}


}
