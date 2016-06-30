package org.springframework.cloud.stream.schema;

import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;


/**
 * @author Vinicius Carvalho
 */
public class AvroVersioningTests {

	Schema v1;
	Schema v2;
	GenericRecord sensor1;
	GenericRecord sensor2;
	SchemaRegistryClient client;

	@Before
	public void setup() throws Exception{
		v1 = new Schema.Parser().parse(AvroVersioningTests.class.getClassLoader().getResourceAsStream("src/test/resources/schemas/sensor.v1.avsc"));
		v2 = new Schema.Parser().parse(AvroVersioningTests.class.getClassLoader().getResourceAsStream("src/test/resources/schemas/sensor.v2.avsc"));
		sensor1 = new GenericData.Record(v1);
		sensor1.put("id","v1");
		sensor1.put("velocity",0.1f);
		sensor1.put("acceleration",0.1f);
		sensor1.put("accelerometer", Arrays.asList(0.1f, 0.1f, 0.1f));
		sensor1.put("magneticField", Arrays.asList(0.1f, 0.1f, 0.1f));
		sensor1.put("orientation", Arrays.asList(0.1f, 0.1f, 0.1f));
		sensor1.put("temperature",0.1f);

		sensor2 = new GenericData.Record(v2);

		sensor2.put("id","v2");
		sensor2.put("velocity",0.2f);
		sensor2.put("acceleration",0.2f);
		sensor2.put("accelerometer", Arrays.asList(0.2f, 0.2f, 0.2f));
		sensor2.put("magneticField", Arrays.asList(0.2f, 0.2f, 0.2f));
		sensor2.put("internalTemperature",0.2f);
		sensor2.put("externalTemperature",0.2f);

		client = mock(SchemaRegistryClient.class);
		when(client.register(eq(v1))).thenReturn(1);
		when(client.register(eq(v2))).thenReturn(2);
		when(client.fetch(eq(1))).thenReturn(v1);
		when(client.fetch(eq(2))).thenReturn(v2);



	}


	@Test
	public void writeV1ReadV2() throws Exception{

	}

}
