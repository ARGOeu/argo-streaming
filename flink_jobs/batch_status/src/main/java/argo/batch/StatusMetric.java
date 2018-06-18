package argo.batch;


public class StatusMetric {

	
	private String group;
	private String service;
	private String hostname;
	private String metric;
	private String status;
	private String timestamp;
	private int dateInt;
	private int timeInt;
	private String summary;
	private String message;
	private String prevState;
	private String prevTs;
	private String actualData;
	
	public StatusMetric(){
		this.group = "";
		this.service ="";
		this.hostname = "";
		this.metric="";
		this.status = "";
		this.timestamp = "";
		this.dateInt = 0;
		this.timeInt =0;
		this.summary = "";
		this.message = "";
		this.prevState = "";
		this.prevTs = "";
		this.actualData = "";
	}
	
	public StatusMetric(String group, String service, String hostname, String metric, String status, String timestamp,
			int dateInt, int timeInt, String summary, String message, String prevState, String prevTs, String actualData) {
		
		this.group = group;
		this.service = service;
		this.hostname = hostname;
		this.metric = metric;
		this.status = status;
		this.timestamp = timestamp;
		this.dateInt = dateInt;
		this.timeInt = timeInt;
		this.summary = summary;
		this.message = message;
		this.prevState = prevState;
		this.prevTs = prevTs;
		this.actualData = actualData;
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
	public int getDateInt() {
		return dateInt;
	}
	public void setDateInt(int dateInt) {
		this.dateInt = dateInt;
	}
	public int getTimeInt() {
		return timeInt;
	}
	public void setTimeInt(int timeInt) {
		this.timeInt = timeInt;
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
	
	public String getPrevState() {
		return prevState;
	}
	public void setPrevState(String prevState) {
		this.prevState = prevState;
	}
	public String getPrevTs() {
		return prevTs;
	}
	public void setPrevTs(String prevTs) {
		this.prevTs = prevTs;
	}
	
	public String getActualData() {
		return actualData;
	}
	public void setActualData(String actualData) {
		this.actualData = actualData;
	}
	
	@Override
	public String toString() {
		return "(" + this.group + "," + this.service + "," + this.hostname + "," + this.metric + "," + this.status + "," + this.timestamp + "," + 
				this.dateInt + "," + this.timeInt + "," + this.prevState + "," + this.prevTs + "," + this.actualData +  ")";
	}
	
}
