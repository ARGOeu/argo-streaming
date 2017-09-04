package argo.batch;

import java.util.Arrays;

/* Extends the metric data information by adding the extra group field
 * 
 */
public class MonData {

	private String group;
	private String service;
	private String hostname;
	private String metric;
	private String status;
	private String timestamp;
	private String monHost;
	private String summary;
	private String message;

	
	MonData(){
		this.group="";
		this.service="";
		this.hostname="";
		this.metric="";
		this.status="";
		this.timestamp="";
		this.monHost="";
		this.summary="";
		this.message="";
	}
	
	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getService() {
		return service;
	}

	public void setService(String service) {
		this.service = service;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getMetric() {
		return metric;
	}

	public void setMetric(String metric) {
		this.metric = metric;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String getMonHost() {
		return monHost;
	}

	public void setMonHost(String monHost) {
		this.monHost = monHost;
	}

	public String getSummary() {
		return summary;
	}

	public void setSummary(String summary) {
		this.summary = summary;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String toString() {
		return "(" + this.group + "," + this.service + "," + this.hostname + "," + this.metric + "," + this.status + ","
				+ this.timestamp + "," + this.monHost + "," + this.summary + "," + this.message + ")";
	}

}
