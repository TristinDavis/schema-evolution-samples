package org.springframework.cloud.stream.codec.avro;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.cloud.stream.schema.SchemaRegistryClient;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Vinicius Carvalho
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(ConfluentSchemaClientApplication.class)
@IntegrationTest({ "confluent.schemaregistry.endpoint: http://192.168.99.100:8081" })
@DirtiesContext
public class ConfluentSchemaClientIntegrationIT extends AbstractAvroTestCase{

	@Autowired
	private SchemaRegistryClient client;

	@Test
	public void registerSchema() throws Exception{
		Schema status = load("status.avsc");
		Integer id = client.register(status);
		Schema loadedSchema = client.fetch(id);
		Assert.assertTrue(status.equals(loadedSchema));
	}


}


@Configuration
@EnableAutoConfiguration
class ConfluentSchemaClientApplication {

}