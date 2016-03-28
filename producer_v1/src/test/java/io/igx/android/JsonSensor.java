package io.igx.android;

import java.util.List;

/**
 * @author Vinicius Carvalho
 */
public class JsonSensor {



	private String id;
	private float temperature;
	private float velocity;
	private float acceleration;
	private List<Float> accelerometer;
	private List<Float> magneticField;
	private List<Float> orientation;

	public JsonSensor(){}

	public JsonSensor(Sensor sensor) {
		this.id = sensor.getId().toString();
		this.acceleration = sensor.getAcceleration();
		this.temperature = sensor.getTemperature();
		this.velocity = sensor.getVelocity();
		this.magneticField = sensor.getMagneticField();
		this.orientation = sensor.getOrientation();
		this.accelerometer = sensor.getAccelerometer();

	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public float getTemperature() {
		return temperature;
	}

	public void setTemperature(float temperature) {
		this.temperature = temperature;
	}

	public float getVelocity() {
		return velocity;
	}

	public void setVelocity(float velocity) {
		this.velocity = velocity;
	}

	public float getAcceleration() {
		return acceleration;
	}

	public void setAcceleration(float acceleration) {
		this.acceleration = acceleration;
	}

	public List<Float> getAccelerometer() {
		return accelerometer;
	}

	public void setAccelerometer(List<Float> accelerometer) {
		this.accelerometer = accelerometer;
	}

	public List<Float> getMagneticField() {
		return magneticField;
	}

	public void setMagneticField(List<Float> magneticField) {
		this.magneticField = magneticField;
	}

	public List<Float> getOrientation() {
		return orientation;
	}

	public void setOrientation(List<Float> orientation) {
		this.orientation = orientation;
	}
}
