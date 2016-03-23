package org.springframework.cloud.stream.codec.avro;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.cloud.stream.schema.SchemaRegistryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Vinicius Carvalho
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(SampleAvroApplication.class)
@IntegrationTest({ "spring.cloud.stream.codec.avro.dynamicSchemaGenerationEnabled: true" })
@DirtiesContext
public class AvroCodecBootTests {
	@Test
	public void contextLoads() throws Exception{}

}

@EnableAutoConfiguration
@Configuration
class SampleAvroApplication{

	@Bean
	public SchemaRegistryClient schemaRegistryClient(){
		SchemaRegistryClient client = Mockito.mock(SchemaRegistryClient.class);
		return client;
	}

}