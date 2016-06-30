package org.springframework.cloud.stream.converters.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.schema.SchemaNotFoundException;
import org.springframework.cloud.stream.schema.SchemaRegistryClient;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * @author Vinicius Carvalho
 */
public class AvroMessageConverter implements MessageConverter {

	private SchemaRegistryClient registryClient;
	private Map<String,Schema> localSchemaMap;
	private AvroConverterProperties properties;
	private ResourcePatternResolver resolver;
	private Schema readerSchema;
	private Logger logger = LoggerFactory.getLogger(AvroMessageConverter.class);

	public AvroMessageConverter(SchemaRegistryClient registryClient, AvroConverterProperties properties, ApplicationContext ctx) {
		this.registryClient = registryClient;
		this.properties = properties;
		this.resolver =  new PathMatchingResourcePatternResolver(ctx);
		this.localSchemaMap = new ConcurrentHashMap<>();
	}

	@Override
	public Object fromMessage(Message<?> message, Class<?> targetClass) {
		Schema writerSchema = resolveSchema(message.getHeaders());
		DatumReader reader = getDatumReader(targetClass,writerSchema);
		byte[] payload = (byte[])message.getPayload();
		Decoder decoder = DecoderFactory.get().binaryDecoder(payload,null);
		Object result = null;
		try {
			result =  reader.read(null,decoder);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
		return  result;
	}

	@Override
	public Message<?> toMessage(Object payload, MessageHeaders headers) {
		Schema schema = getSchema(payload);
		DatumWriter writer = getDatumWriter(payload.getClass(),schema);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
		Message message = null;
		try {
			writer.write(payload,encoder);
			encoder.flush();
			MessageBuilder builder = MessageBuilder.withPayload(baos.toByteArray()).copyHeaders(headers);
			if(registryClient != null){
				builder.setHeader("X-Schema-Id",registryClient.register(schema));
			}else{
				builder.setHeader("X-Schema-Name",payload.getClass().getName());
			}
			message = builder.build();
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}

		return message;
	}




	private Schema resolveSchema(MessageHeaders headers){
		if(headers.get("X-Schema-Id") != null){
			return registryClient.fetch((Integer) headers.get("X-Schema-Id"));
		}else{
			return localSchemaMap.get(headers.get("X-Schema-Name"));
		}
	}


	private Schema getSchema(Object payload){
		Schema schema = null;
		logger.debug("Obtaining schema for class {}", payload.getClass());
		if(GenericContainer.class.isAssignableFrom(payload.getClass())) {
			schema = ((GenericContainer) payload).getSchema();
			logger.debug("Avro type detected, using schema from object");
		}else{
			schema = localSchemaMap.get(payload.getClass().getName());
			if(schema == null){
				if(!properties.isDynamicSchemaGenerationEnabled()) {
					throw new SchemaNotFoundException(String.format("No schema found on local cache for %s", payload.getClass()));
				}
				else{
					schema = ReflectData.get().getSchema(payload.getClass());
					localSchemaMap.put(payload.getClass().getName(),schema);
					if(registryClient != null){
						registryClient.register(schema);
					}
				}
			}
		}

		return schema;
	}

	private DatumWriter getDatumWriter(Class<?> type, Schema schema){
		DatumWriter writer = null;
		logger.debug("Finding correct DatumWriter for type {}",type.getName());
		if(SpecificRecord.class.isAssignableFrom(type)){
			writer = new SpecificDatumWriter<>(schema);
		}else if(GenericRecord.class.isAssignableFrom(type)){
			writer = new GenericDatumWriter<>(schema);
		}else{
			writer = new ReflectDatumWriter<>(schema);
		}
		logger.debug("DatumWriter of type {} selected",writer.getClass().getName());
		return writer;
	}

	private DatumReader getDatumReader(Class<?> type, Schema writer){
		DatumReader reader = null;
		if(SpecificRecord.class.isAssignableFrom(type)){
			reader = new SpecificDatumReader<>(writer,getReaderSchema(writer));
		}
		else if(GenericRecord.class.isAssignableFrom(type)){
			reader = new GenericDatumReader<>(writer,getReaderSchema(writer));
		}else{
			reader = new ReflectDatumReader<>(writer,getReaderSchema(writer));
		}

		return reader;
	}

	private Schema getReaderSchema(Schema writerSchema){
		return readerSchema != null ? readerSchema :writerSchema;
	}

	@PostConstruct
	public void init() {
		logger.info("Scanning avro schema resources on classpath");
		Schema.Parser parser = new Schema.Parser();
		try {
			Resource[] resources = resolver.getResources("classpath*:/**/*.avsc");
			logger.info("Found {} schemas on classpath",resources.length);
			for(Resource r : resources){
				Schema s = parser.parse(r.getInputStream());
				if(!StringUtils.isEmpty(properties.getReaderSchema()) && properties.getReaderSchema().equals(s.getFullName())){
					readerSchema = s;
				}
				logger.info("Resource {} parsed into schema {}.{}",r.getFilename(), s.getNamespace(), s.getName());
				logger.info("Schema {} registered with id {}",s.getName(),s);
				localSchemaMap.put(s.getNamespace()+"."+s.getName(),s);
				if(registryClient != null){
					registryClient.register(s);
				}
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}

	}

}
