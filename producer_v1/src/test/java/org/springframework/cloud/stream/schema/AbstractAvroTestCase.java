package org.springframework.cloud.stream.schema;

import java.io.InputStream;

import org.apache.avro.Schema;

/**
 * @author Vinicius Carvalho
 */
public abstract class AbstractAvroTestCase {

	protected static Schema load(String name) throws Exception{
		InputStream in = AbstractAvroTestCase.class.getClassLoader().getResourceAsStream(name);
		Schema.Parser parser = new Schema.Parser();
		return parser.parse(in);
	}
}
