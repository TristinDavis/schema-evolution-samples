package org.springframework.cloud.stream.schema;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.apache.avro.Schema;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Vinicius Carvalho
 */
public class ConfluentSchemaRegistryClient implements SchemaRegistryClient {

	private RestTemplate template;
	private final String endpoint;
	private ObjectMapper mapper;
	public ConfluentSchemaRegistryClient(String endpoint){
		template = new RestTemplate();
		this.endpoint = endpoint;
		this.mapper = new ObjectMapper();
	}

	@Override
	public Integer register(Schema schema) {
		String path = String.format("/subjects/%s/versions",schema.getFullName());
		HttpHeaders headers = new HttpHeaders();
		headers.put("Accept", Arrays.asList("application/vnd.schemaregistry.v1+json","application/vnd.schemaregistry+json","application/json"));
		headers.add("Content-Type","application/json");
		Integer id = null;
		try {
			String payload = mapper.writeValueAsString(Collections.singletonMap("schema",schema.toString()));
			HttpEntity<String> request = new HttpEntity<>(payload,headers);
			ResponseEntity<Map> response = template.exchange(endpoint+path, HttpMethod.POST,request, Map.class);
			id = (Integer)response.getBody().get("id");
		}
		catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return id;
	}

	@Override
	public Schema fetch(Integer id) {
		String path = String.format("/schemas/ids/%d",id);
		HttpHeaders headers = new HttpHeaders();
		headers.put("Accept", Arrays.asList("application/vnd.schemaregistry.v1+json","application/vnd.schemaregistry+json","application/json"));
		headers.add("Content-Type","application/vnd.schemaregistry.v1+json");
		HttpEntity<String> request = new HttpEntity<>("",headers);
		ResponseEntity<Map> response = template.exchange(endpoint+path, HttpMethod.GET,request, Map.class);
		String schemaString = (String) response.getBody().get("schema");
		Schema.Parser parser = new Schema.Parser();
		return parser.parse(schemaString);
	}
}
