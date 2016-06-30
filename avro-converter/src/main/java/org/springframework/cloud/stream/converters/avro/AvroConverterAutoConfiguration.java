package org.springframework.cloud.stream.converters.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.io.DatumWriter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.converter.AbstractFromMessageConverter;
import org.springframework.cloud.stream.schema.CachingSchemaRegistryClient;
import org.springframework.cloud.stream.schema.ConfluentSchemaRegistryClient;
import org.springframework.cloud.stream.schema.SchemaRegistryClient;
import org.springframework.cloud.stream.schema.SpringSchemaRegistry;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.messaging.Message;
import org.springframework.util.MimeType;

/**
 * @author Vinicius Carvalho
 */
@ConditionalOnClass(DatumWriter.class)
@Configuration
@EnableConfigurationProperties({AvroConverterProperties.class})
public class AvroConverterAutoConfiguration {

	@Autowired(required = false)
	private SchemaRegistryClient schemaRegistryClient;


	@Bean
	@ConditionalOnProperty("confluent.schemaregistry.endpoint")
	public SchemaRegistryClient confluentClient(@Value("${confluent.schemaregistry.endpoint}") String endpoint){
		ConfluentSchemaRegistryClient registryClient = new ConfluentSchemaRegistryClient(endpoint);
		return new CachingSchemaRegistryClient(registryClient);
	}

	@Bean
	@ConditionalOnProperty("spring.cloud.schema.registry")
	public SchemaRegistryClient springClient(@Value("${spring.cloud.schema.registry}") String endpoint){
		SpringSchemaRegistry registryClient = new SpringSchemaRegistry(endpoint);
		return new CachingSchemaRegistryClient(registryClient);
	}

	@Bean
	public PojoToAvroMessageConverter pojoToAvroMessageConverter(AvroMessageConverter converter) throws Exception{
		return new PojoToAvroMessageConverter(converter);
	}

	@Bean
	public AvroToPojoMessageConverter avroToPojoMessageConverter(AvroMessageConverter converter) throws Exception{
		return new AvroToPojoMessageConverter(converter);
	}

	@Bean
	@Autowired(required = false)
	public AvroMessageConverter avroMessageConverter(ApplicationContext ctx, AvroConverterProperties properties){
		return new AvroMessageConverter(schemaRegistryClient,properties,ctx);
	}


}

class PojoToAvroMessageConverter extends AbstractFromMessageConverter{

	private AvroMessageConverter converter;

	public PojoToAvroMessageConverter(AvroMessageConverter converter) {
		super(MimeType.valueOf("avro/binary"));
		this.converter = converter;
	}

	@Override
	protected Class<?>[] supportedTargetTypes() {
		return new Class[] { byte[].class };

	}

	@Override
	protected Class<?>[] supportedPayloadTypes() {
		return null;
	}

	@Override
	protected Object convertFromInternal(Message<?> message, Class<?> targetClass, Object conversionHint) {
		return converter.toMessage(message.getPayload(),message.getHeaders());
	}
}

class AvroToPojoMessageConverter extends AbstractFromMessageConverter{

	private AvroMessageConverter converter;

	public AvroToPojoMessageConverter(AvroMessageConverter converter) {
		super(MimeType.valueOf("avro/binary"));
		this.converter = converter;
	}

	@Override
	protected Class<?>[] supportedTargetTypes() {
		return null;

	}

	@Override
	protected Class<?>[] supportedPayloadTypes() {
		return new Class[] { byte[].class };
	}

	@Override
	protected Object convertFromInternal(Message<?> message, Class<?> targetClass, Object conversionHint) {
		return converter.fromMessage(message,targetClass);
	}
}