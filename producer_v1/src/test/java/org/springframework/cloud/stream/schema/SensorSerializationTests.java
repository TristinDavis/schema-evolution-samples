package org.springframework.cloud.stream.schema;

import java.io.File;

import io.igx.android.Sensor;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * @author Vinicius Carvalho
 */
public class SensorSerializationTests {

	public void serialize() throws Exception{
		Sensor sensor = Sensor.newBuilder().build();
		SpecificDatumWriter<Sensor> writer = new SpecificDatumWriter<>(Sensor.class);
		DataFileWriter<Sensor> dataFileWriter = new DataFileWriter<>(writer);
		dataFileWriter.create(sensor.getSchema(),new File("sensors.dat"));
		dataFileWriter.append(sensor);
		dataFileWriter.close();
	}

	public void genericSerialize() throws Exception {
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse("sensor.avsc");
		GenericRecord sensor = new GenericData.Record(schema);
		sensor.put("temperature",21.5);
		sensor.put("acceleration",3.7);
		GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer);
		dataFileWriter.create(schema,new File("sensors.dat"));
		dataFileWriter.append(sensor);
		dataFileWriter.close();
	}
}
