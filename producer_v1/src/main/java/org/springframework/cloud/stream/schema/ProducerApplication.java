package org.springframework.cloud.stream.schema;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author Vinicius Carvalho
 */
@SpringBootApplication
public class ProducerApplication {
	public static void main(String[] args) {
		SpringApplication.run(ProducerApplication.class,args);
	}
}
