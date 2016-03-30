package schema.sink;

import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;

import io.igx.android.Sensor;
import org.apache.avro.generic.GenericRecord;

/**
 * @author Vinicius Carvalho
 */
@EnableBinding(Sink.class)
public class SensorSink {

	@StreamListener(Sink.INPUT)
	public void process(GenericRecord data){
		System.out.println(data);

	}
}
