package org.springframework.cloud.stream.schema;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import io.igx.android.JsonSensor;
import io.igx.android.Sensor;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Vinicius Carvalho
 */
public class AvroJacksonBenchmarks {

	private SpecificAvroSerializer avroSerializer = new SpecificAvroSerializer();
	private JacksonSerializer jacksonSerializer = new JacksonSerializer();
	private Random random = new Random();
	private Clock clock = Clock.systemUTC();

	private Sensor sensor;
	private JsonSensor jsonSensor;

	private byte[] avroBytes;
	private byte[] jsonBytes;

	private final int iterations = 10000;
	private final int warmup = 1000;
	private final int samples = 10;

	@Before
	public void setup() throws Exception{
		this.sensor = createSensorData();
		this.jsonSensor = new JsonSensor(sensor);
		this.avroBytes = avroSerializer.write(sensor);
		this.jsonBytes = jacksonSerializer.write(jsonSensor);
	}

	@Test
	public void encode() throws Exception {
		warmWrite(avroSerializer,sensor);
		warmWrite(jacksonSerializer,jsonSensor);
		List<Double> avroWrites = new ArrayList<>(samples);
		List<Double> jacksonWrites = new ArrayList<>(samples);

		for(int i=0;i<samples;i++){
			avroWrites.add(benchmarkWrite(avroSerializer,sensor));
			jacksonWrites.add(benchmarkWrite(jacksonSerializer,jsonSensor));
		}
		System.out.println(String.format("Avro write mean: %f, Jackson write mean: %f",average(avroWrites),average(jacksonWrites)));
	}

	@Test
	public void decode() throws Exception {
		warmRead(avroSerializer,avroBytes);
		warmRead(jacksonSerializer,jsonBytes);
		List<Double> avroWrites = new ArrayList<>(samples);
		List<Double> jacksonWrites = new ArrayList<>(samples);
		for(int i=0;i<samples;i++){
			avroWrites.add(benchmarRead(avroSerializer,avroBytes));
			jacksonWrites.add(benchmarRead(jacksonSerializer,jsonBytes));
		}

		System.out.println(String.format("Avro read mean: %f, Jackson read mean: %f",average(avroWrites),average(jacksonWrites)));
	}

	private void warmWrite(Serializer serializer, Object type) throws Exception {
		for(int i=0;i<warmup;i++){
			serializer.write(type);
		}
	}

	private void warmRead(Serializer serializer, byte[] bytes) throws Exception{
		for(int i=0;i<warmup;i++){
			serializer.read(bytes);
		}
	}

	private Double benchmarRead(Serializer serializer, byte[] bytes) throws Exception{
		Double total = 0.0;
		for(int i=0;i<iterations;i++){
			Instant start = clock.instant();
			serializer.read(bytes);
			Instant end = clock.instant();
			total+= (end.getNano()-start.getNano());
		}
		return total/iterations;
	}

	private Double benchmarkWrite(Serializer serializer, Object type) throws Exception{
		Double total = 0.0;
		for(int i=0;i<iterations;i++){
			Instant start = clock.instant();
			serializer.write(type);
			Instant end = clock.instant();
			total+= (end.getNano()-start.getNano());
		}
		return total/iterations;
	}

	private Double average(List<Double> values){
		Collections.sort(values,(o1, o2) -> o1.compareTo(o2));
		List<Double> filtered = values.subList((int)(0.2*values.size()),(int)(0.8*values.size()));
		DescriptiveStatistics dStats = new DescriptiveStatistics(filtered.stream().mapToDouble(Double::doubleValue).toArray());

		return dStats.getMean();
	}


	private Sensor createSensorData(){
		Sensor sensor = new Sensor();
		sensor.setTemperature(random.nextFloat());
		sensor.setAcceleration(random.nextFloat());
		sensor.setId(UUID.randomUUID().toString());
		sensor.setVelocity(random.nextFloat());
		sensor.setOrientation(Arrays.asList(random.nextFloat(),random.nextFloat(),random.nextFloat()));
		sensor.setAccelerometer(Arrays.asList(random.nextFloat(),random.nextFloat(),random.nextFloat()));
		sensor.setMagneticField(Arrays.asList(random.nextFloat(),random.nextFloat(),random.nextFloat()));

		return sensor;
	}
}
