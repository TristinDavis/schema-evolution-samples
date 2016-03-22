package org.springframework.cloud.stream.codec.avro;

import example.avro.User;
import org.apache.avro.Schema;
import static org.mockito.Mockito.*;

import java.util.ArrayList;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.junit.Assert;
import org.junit.Test;

import org.springframework.cloud.stream.samples.Account;
import org.springframework.cloud.stream.samples.Status;
import org.springframework.cloud.stream.schema.SchemaNotFoundException;
import org.springframework.cloud.stream.schema.SchemaRegistryClient;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

/**
 * @author Vinicius Carvalho
 */
public class AvroCodecTests extends AbstractAvroTestCase{

	@Test
	public void genericEncoderV1GenericDecoderV1() throws Exception{
		Schema schema = load("users_v1.schema");
		SchemaRegistryClient client = mock(SchemaRegistryClient.class);
		AvroCodec codec = new AvroCodec();
		codec.setSchemaRegistryClient(client);
		when(client.register(any())).thenReturn(1);
		when(client.fetch(eq(1))).thenReturn(schema);
		GenericRecord record = new GenericData.Record(schema);
		record.put("name","joe");
		record.put("favoriteNumber",42);
		record.put("favoriteColor","blue");
		byte[] results = codec.encode(record);
		GenericRecord decoded = codec.decode(results,GenericRecord.class);
		Assert.assertEquals(record.get("name").toString(),decoded.get("name").toString());
	}

	@Test
	public void genericEncoderV2GenericDecoderV2() throws Exception{
		Schema schema = load("users_v2.schema");
		SchemaRegistryClient client = mock(SchemaRegistryClient.class);
		AvroCodec codec = new AvroCodec();
		codec.setSchemaRegistryClient(client);
		when(client.register(any())).thenReturn(2);
		when(client.fetch(eq(2))).thenReturn(schema);
		GenericRecord record = new GenericData.Record(schema);
		record.put("name","joe");
		record.put("favoriteNumber",42);
		record.put("favoriteColor","blue");
		record.put("favoritePlace","Paris");
		byte[] results = codec.encode(record);
		GenericRecord decoded = codec.decode(results,GenericRecord.class);
		Assert.assertEquals(record.get("favoritePlace").toString(),decoded.get("favoritePlace").toString());
	}

	@Test
	public void genericEncoderV2GenericDecoderV1() throws Exception{
		Schema reader = load("users_v1.schema");
		Schema writer = load("users_v2.schema");
		SchemaRegistryClient client = mock(SchemaRegistryClient.class);
		AvroCodec codec = new AvroCodec();
		codec.setReaderSchema(reader);
		codec.setSchemaRegistryClient(client);
		when(client.register(any())).thenReturn(2);
		when(client.fetch(eq(2))).thenReturn(writer);
		GenericRecord record = new GenericData.Record(writer);
		record.put("name","joe");
		record.put("favoriteNumber",42);
		record.put("favoriteColor","blue");
		record.put("favoritePlace","Paris");
		byte[] results = codec.encode(record);
		GenericRecord decoded = codec.decode(results,GenericRecord.class);
		Assert.assertEquals(record.get("name").toString(),decoded.get("name").toString());
	}

	@Test
	public void genericEncoderV1GenericDecoderV2() throws Exception{
		Schema reader = load("users_v2.schema");
		Schema writer = load("users_v1.schema");
		SchemaRegistryClient client = mock(SchemaRegistryClient.class);
		AvroCodec codec = new AvroCodec();
		codec.setReaderSchema(reader);
		codec.setSchemaRegistryClient(client);
		when(client.register(any())).thenReturn(2);
		when(client.fetch(eq(2))).thenReturn(writer);
		GenericRecord record = new GenericData.Record(writer);
		record.put("name","joe");
		record.put("favoriteNumber",42);
		record.put("favoriteColor","blue");
		byte[] results = codec.encode(record);
		GenericRecord decoded = codec.decode(results,GenericRecord.class);
		Assert.assertEquals(record.get("name").toString(),decoded.get("name").toString());
		Assert.assertEquals("NYC",decoded.get("favoritePlace").toString());
	}

	@Test
	public void genericEncoderV1SpecificDecoderV1() throws Exception{
		Schema schema = load("users_v1.schema");
		SchemaRegistryClient client = mock(SchemaRegistryClient.class);
		AvroCodec codec = new AvroCodec();
		codec.setSchemaRegistryClient(client);
		when(client.register(any())).thenReturn(1);
		when(client.fetch(eq(1))).thenReturn(schema);
		GenericRecord record = new GenericData.Record(schema);
		record.put("name","joe");
		record.put("favoriteNumber",42);
		record.put("favoriteColor","blue");
		byte[] results = codec.encode(record);
		User decoded = codec.decode(results,User.class);
		Assert.assertEquals(record.get("name").toString(),decoded.getName().toString());

	}

	@Test
	public void specificEncoderV1GenericDecoderV1() throws Exception{
		Schema schema = load("users_v1.schema");
		SchemaRegistryClient client = mock(SchemaRegistryClient.class);
		AvroCodec codec = new AvroCodec();
		codec.setSchemaRegistryClient(client);
		when(client.register(any())).thenReturn(1);
		when(client.fetch(eq(1))).thenReturn(schema);
		User user = User.newBuilder().setName("joe").setFavoriteColor("blue").setFavoriteNumber(42).build();
		byte[] results = codec.encode(user);
		GenericRecord decoded = codec.decode(results,GenericRecord.class);
		Assert.assertEquals(user.getName().toString(),decoded.get("name").toString());
	}


	@Test
	public void reflectEncoderGenericDecoder() throws Exception{
		SchemaRegistryClient client = mock(SchemaRegistryClient.class);
		Schema schema = load("status.avsc");
		when(client.register(any())).thenReturn(10);
		when(client.fetch(eq(10))).thenReturn(schema);
		AvroCodec codec = new AvroCodec();
		codec.setSchemaRegistryClient(client);
		codec.setResolver(new PathMatchingResourcePatternResolver(new AnnotationConfigApplicationContext()));
		codec.setProperties(new AvroCodecProperties());
		codec.init();
		Status status = new Status("1","sample",System.currentTimeMillis());
		byte[] results = codec.encode(status);
		GenericRecord decoded = codec.decode(results,GenericRecord.class);
		Assert.assertEquals(status.getId(),decoded.get("id").toString());
	}

	@Test
	public void reflectEncoderReflectDecoder() throws Exception{
		SchemaRegistryClient client = mock(SchemaRegistryClient.class);
		Schema schema = load("status.avsc");
		when(client.register(any())).thenReturn(10);
		when(client.fetch(eq(10))).thenReturn(schema);
		AvroCodec codec = new AvroCodec();
		codec.setSchemaRegistryClient(client);
		codec.setResolver(new PathMatchingResourcePatternResolver(new AnnotationConfigApplicationContext()));
		codec.setProperties(new AvroCodecProperties());
		codec.init();
		Status status = new Status("1","sample",System.currentTimeMillis());
		byte[] results = codec.encode(status);
		Status decoded = codec.decode(results,Status.class);
		Assert.assertEquals(status.getId(),decoded.getId());
	}

	@Test
	public void genericEncoderReflectDecoder() throws Exception{
		SchemaRegistryClient client = mock(SchemaRegistryClient.class);
		Schema schema = load("status.avsc");
		when(client.register(any())).thenReturn(10);
		when(client.fetch(eq(10))).thenReturn(schema);
		AvroCodec codec = new AvroCodec();
		codec.setSchemaRegistryClient(client);
		codec.setResolver(new PathMatchingResourcePatternResolver(new AnnotationConfigApplicationContext()));
		codec.setProperties(new AvroCodecProperties());
		codec.init();
		GenericRecord record = new GenericData.Record(schema);
		record.put("id","1");
		record.put("text","sample");
		record.put("timestamp",System.currentTimeMillis());
		byte[] results = codec.encode(record);
		Status status = codec.decode(results,Status.class);
		Assert.assertEquals(record.get("id").toString(),status.getId());
	}

	@Test(expected = SchemaNotFoundException.class)
	public void localSchemaNotFound() throws Exception{
		AvroCodec codec = new AvroCodec();
		SchemaRegistryClient client = mock(SchemaRegistryClient.class);
		codec.setProperties(new AvroCodecProperties());
		codec.setSchemaRegistryClient(client);
		codec.setResolver(new PathMatchingResourcePatternResolver(new AnnotationConfigApplicationContext()));
		codec.encode(new ArrayList<>());

	}

	@Test
	public void dynamicReflectEncoderReflectDecoder() throws Exception{
		Schema schema = ReflectData.get().getSchema(Account.class);
		SchemaRegistryClient client = mock(SchemaRegistryClient.class);
		when(client.register(any())).thenReturn(10);
		when(client.fetch(eq(10))).thenReturn(schema);
		AvroCodec codec = new AvroCodec();
		AvroCodecProperties properties = new AvroCodecProperties();
		properties.setDynamicSchemaGenerationEnabled(true);
		codec.setProperties(properties);
		codec.setSchemaRegistryClient(client);
		codec.setResolver(new PathMatchingResourcePatternResolver(new AnnotationConfigApplicationContext()));
		codec.init();
		Account account = new Account();
		account.setCreatedAt(System.currentTimeMillis());
		account.setId(1L);
		byte[] results = codec.encode(account);
		Account decoded = codec.decode(results,Account.class);
		Assert.assertEquals(account.getId(), decoded.getId());
	}


}
