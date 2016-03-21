package org.springframework.cloud.stream.codec.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.schema.SchemaNotFoundException;
import org.springframework.cloud.stream.schema.SchemaRegistryClient;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.integration.codec.Codec;
import org.springframework.util.Assert;

/**
 * @author Vinicius Carvalho
 */
public class AvroCodec implements Codec {

	private SchemaRegistryClient schemaRegistryClient;

	private Schema readerSchema;

	private ResourcePatternResolver resolver;

	private Map<String,Integer> localSchemaMap;

	private Logger logger = LoggerFactory.getLogger(AvroCodec.class);

	private AvroCodecProperties properties;

	public AvroCodec(){
		this.localSchemaMap = new ConcurrentHashMap<>();
	}

	@Override
	public void encode(Object object, OutputStream outputStream) throws IOException {
		Schema schema = getSchema(object);
		Integer id = schemaRegistryClient.register(schema);
		DatumWriter writer = getDatumWriter(object.getClass(),schema);
		Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
		outputStream.write(ByteBuffer.allocate(4).putInt(id).array());
		writer.write(object,encoder);
		encoder.flush();
	}

	@Override
	public byte[] encode(Object o) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		encode(o,baos);
		return baos.toByteArray();
	}

	@Override
	public <T> T decode(InputStream inputStream, Class<T> type) throws IOException {
		return decode(IOUtils.toByteArray(inputStream),type);
	}

	@Override
	public <T> T decode(byte[] bytes, Class<T> type) throws IOException {
		Assert.notNull(bytes, "'bytes' cannot be null");
		Assert.notNull(bytes, "Class can not be null");
		ByteBuffer buf = ByteBuffer.wrap(bytes);
		byte[] payload = new byte[bytes.length-4];
		Integer schemaId = buf.getInt();
		buf.get(payload);
		Schema schema = schemaRegistryClient.fetch(schemaId);
		DatumReader reader = getDatumReader(type,schema);
		Decoder decoder = DecoderFactory.get().binaryDecoder(payload,null);
		return (T) reader.read(null,decoder);
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

	private Schema  getSchema(Object payload){
		Schema schema = null;
		logger.debug("Obtaining schema for class {}", payload.getClass());
		if(GenericContainer.class.isAssignableFrom(payload.getClass())) {
			schema = ((GenericContainer) payload).getSchema();
			logger.debug("Avro type detected, using schema from object");
		}else{
			Integer id = localSchemaMap.get(payload.getClass().getName());
			if(id == null){
				if(!properties.isDynamicSchemaGenerationEnabled()) {
					throw new SchemaNotFoundException(String.format("No schema found on local cache for %s", payload.getClass()));
				}
				else{
					Schema localSchema = ReflectData.get().getSchema(payload.getClass());
					id = schemaRegistryClient.register(localSchema);
				}

			}
			schema = schemaRegistryClient.fetch(id);
		}

		return schema;
	}

	public void init() {
		logger.info("Scanning avro schema resources on classpath");
		Schema.Parser parser = new Schema.Parser();
		try {
			Resource[] resources = resolver.getResources("*.avsc");
			logger.info("Found {} schemas on classpath",resources.length);
			for(Resource r : resources){
				Schema s = parser.parse(r.getInputStream());
				logger.info("Resource {} parsed into schema {}.{}",r.getFilename(), s.getNamespace(), s.getName());
				Integer id = schemaRegistryClient.register(s);
				logger.info("Schema {} registered with id {}",s.getName(),id);
				localSchemaMap.put(s.getNamespace()+"."+s.getName(),id);
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Autowired
	public void setSchemaRegistryClient(SchemaRegistryClient schemaRegistryClient) {
		this.schemaRegistryClient = schemaRegistryClient;
	}

	@Autowired
	public void setReaderSchema(Schema readerSchema) {
		this.readerSchema = readerSchema;
	}

	@Autowired
	public void setResolver(ResourcePatternResolver resolver) {
		this.resolver = resolver;
	}

	@Autowired
	public void setProperties(AvroCodecProperties properties) {
		this.properties = properties;
	}
}
