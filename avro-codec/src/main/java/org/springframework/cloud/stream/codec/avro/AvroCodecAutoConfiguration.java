package org.springframework.cloud.stream.codec.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.io.DatumWriter;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.converter.AbstractFromMessageConverter;
import org.springframework.cloud.stream.schema.CachingSchemaRegistryClient;
import org.springframework.cloud.stream.schema.ConfluentSchemaRegistryClient;
import org.springframework.cloud.stream.schema.SchemaRegistryClient;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.integration.codec.Codec;
import org.springframework.messaging.Message;
import org.springframework.util.MimeType;

/**
 * @author Vinicius Carvalho
 */
@Configuration
@EnableConfigurationProperties({AvroCodecProperties.class})
@ConditionalOnClass(DatumWriter.class)
public class AvroCodecAutoConfiguration {

	//@Bean
	public Codec avroCodec(AvroCodecProperties properties, SchemaRegistryClient schemaRegistryClient, ApplicationContext ctx) throws Exception{
		AvroCodec codec = new AvroCodec();
		codec.setProperties(properties);
		codec.setSchemaRegistryClient(schemaRegistryClient);
		codec.setResolver(new PathMatchingResourcePatternResolver(ctx));
		codec.init();
		return codec;
	}

	@Bean
	public PojoToAvroMessageConverter avroMessageConverter(AvroCodecProperties properties, SchemaRegistryClient schemaRegistryClient, ApplicationContext ctx) throws Exception{
		AvroCodec codec = new AvroCodec();
		codec.setProperties(properties);
		codec.setSchemaRegistryClient(schemaRegistryClient);
		codec.setResolver(new PathMatchingResourcePatternResolver(ctx));
		codec.init();
		return new PojoToAvroMessageConverter(codec);
	}
	@Bean
	public AvroToPojoMessageConverter avroToPojoMessageConverter(AvroCodecProperties properties, SchemaRegistryClient schemaRegistryClient, ApplicationContext ctx) throws Exception{
		AvroCodec codec = new AvroCodec();
		codec.setProperties(properties);
		codec.setSchemaRegistryClient(schemaRegistryClient);
		codec.setResolver(new PathMatchingResourcePatternResolver(ctx));
		codec.init();
		return new AvroToPojoMessageConverter(codec);
	}

	@Bean
	@ConditionalOnProperty("confluent.schemaregistry.endpoint")
	public SchemaRegistryClient confluentClient(@Value("${confluent.schemaregistry.endpoint}") String endpoint){
		ConfluentSchemaRegistryClient registryClient = new ConfluentSchemaRegistryClient(endpoint);
		return new CachingSchemaRegistryClient(registryClient);
	}

//


}

class PojoToAvroMessageConverter extends AbstractFromMessageConverter{

	private AvroCodec codec;

	public PojoToAvroMessageConverter(AvroCodec codec) {
		super(MimeType.valueOf("avro/binary"));
		this.codec = codec;
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
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			codec.encode(message.getPayload(),baos);
		}
		catch (IOException e) {
			return null;
		}
		return baos.toByteArray();
	}
}

class AvroToPojoMessageConverter extends AbstractFromMessageConverter{

	private AvroCodec codec;

	public AvroToPojoMessageConverter(AvroCodec codec) {
		super(MimeType.valueOf("avro/binary"));
		this.codec = codec;
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
		try {
			return codec.decode((byte[]) message.getPayload(),targetClass);
		}
		catch (IOException e) {
			return null;
		}
	}
}
