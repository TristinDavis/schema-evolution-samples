package org.springframework.cloud.stream.schema.sources;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import io.igx.android.Sensor;

import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.core.MessageSource;
import org.springframework.messaging.support.GenericMessage;

/**
 * @author Vinicius Carvalho
 */
@EnableBinding(Source.class)
public class SensorSource {

	private Random random = new Random();
	@Bean
	@InboundChannelAdapter(value = Source.OUTPUT, poller = @Poller(fixedDelay = "${fixedDelay}", maxMessagesPerPoll = "1"))
	public MessageSource<Sensor> sensorSource(){
		return () -> new GenericMessage<Sensor>(randomSensor());
	}

	private Sensor randomSensor(){
		Sensor sensor = new Sensor();
		sensor.setId(UUID.randomUUID().toString());
		sensor.setAcceleration(random.nextFloat()*10);
		sensor.setVelocity(random.nextFloat()*100);
		sensor.setTemperature(random.nextFloat()*50);
		sensor.setAccelerometer(floatArray());
		sensor.setMagneticField(floatArray());
		sensor.setOrientation(floatArray());
		return  sensor;
	}

	private List<Float> floatArray(){
		return Arrays.asList(random.nextFloat(),random.nextFloat(),random.nextFloat());
	}
}
