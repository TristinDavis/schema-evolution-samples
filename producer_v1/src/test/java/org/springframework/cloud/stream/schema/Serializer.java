package org.springframework.cloud.stream.schema;

/**
 * @author Vinicius Carvalho
 */
public interface Serializer<T> {

	public T read(byte[] bytes) throws Exception;
	public byte[] write(T type) throws Exception;

}
