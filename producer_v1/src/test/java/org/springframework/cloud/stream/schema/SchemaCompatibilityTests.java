package org.springframework.cloud.stream.schema;

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.junit.Test;

/**
 * @author Vinicius Carvalho
 */
public class SchemaCompatibilityTests extends AbstractAvroTestCase {

	@Test
	public void printCompatibility() throws Exception {
		Schema v1 = load("src/test/resources/schemas/users_v1.avsc");
		Schema v2 = load("src/test/resources/schemas/users_v2.avsc");
		Schema v3 = load("src/test/resources/schemas/sensor.v3.avsc");

		SchemaCompatibility.SchemaCompatibilityType t1 = SchemaCompatibility.checkReaderWriterCompatibility(v1,v2).getType();
		SchemaCompatibility.SchemaCompatibilityType t2 = SchemaCompatibility.checkReaderWriterCompatibility(v2,v1).getType();

		System.out.println(t1);
		System.out.println(t2);


	}

}
