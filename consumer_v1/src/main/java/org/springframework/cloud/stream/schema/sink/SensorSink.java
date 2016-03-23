package org.springframework.cloud.stream.schema.sink;

import io.igx.android.Sensor;
import org.apache.avro.generic.GenericRecord;

import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;

/**
 * @author Vinicius Carvalho
 */
@EnableBinding(Sink.class)
public class SensorSink {

	@StreamListener(Sink.INPUT)
	public void process(Sensor data){
		System.out.println(data);

	}
}
