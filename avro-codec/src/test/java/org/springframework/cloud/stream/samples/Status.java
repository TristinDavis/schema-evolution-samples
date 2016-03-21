package org.springframework.cloud.stream.samples;

/**
 * Mock class to test reflect serialization
 * @author Vinicius Carvalho
 */
public class Status {

	private String id;
	private String text;
	private Long timestamp;

	public Status(){}

	public Status(String id, String text, Long timestamp) {
		this.id = id;
		this.text = text;
		this.timestamp = timestamp;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}
}
