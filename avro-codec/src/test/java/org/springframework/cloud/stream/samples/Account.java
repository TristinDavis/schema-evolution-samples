package org.springframework.cloud.stream.samples;

/**
 * Mock class to test reflect serialization
 * @author Vinicius Carvalho
 */
public class Account {
	private Long id;
	private Long createdAt;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Long getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Long createdAt) {
		this.createdAt = createdAt;
	}
}
