package org.springframework.cloud.stream.converter.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.cloud.stream.codec.avro.AvroCodec;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.util.MimeType;

/**
 * @author Vinicius Carvalho
 */
public class AvroMessageConverter extends AbstractMessageConverter{



	private Logger logger = LoggerFactory.getLogger(AvroMessageConverter.class);
	private AvroCodec codec;

	public AvroMessageConverter(AvroCodec codec){
		super(MimeType.valueOf("avro/binary"));
		this.codec = codec;
	}

	@Override
	protected boolean supports(Class<?> clazz) {
		return (byte[].class == clazz);
	}

	@Override
	protected Object convertFromInternal(Message<?> message, Class<?> targetClass, Object conversionHint) {
		try {
			return codec.decode((byte[])message.getPayload(),targetClass);
		}
		catch (IOException e) {
			return null;
		}
	}

	@Override
	protected Object convertToInternal(Object payload, MessageHeaders headers, Object conversionHint){
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			codec.encode(payload,baos);
		}
		catch (IOException e) {
			return null;
		}
		return baos.toByteArray();

	}

	public void setCodec(AvroCodec codec) {
		this.codec = codec;
	}
}

