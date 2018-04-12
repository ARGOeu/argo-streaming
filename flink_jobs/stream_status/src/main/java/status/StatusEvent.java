package status;

import com.google.gson.annotations.SerializedName;

public class StatusEvent{

	private String report;
	private String type;
	private @SerializedName("date") String dt;
	private @SerializedName("endpoint_group") String group;
	private String service;
	private String hostname;
	private String metric;
	private String status;
	private @SerializedName("monitoring_host") String monHost;
	private @SerializedName("ts_monitored") String tsMonitored;
	private @SerializedName("ts_processed") String tsProcessed;
	private @SerializedName("prev_status")String prevStatus;
	private @SerializedName("prev_ts")String prevTs;
	private String repeat;
	private String summary;
	private String message;
	
	public StatusEvent (String report, String type, String dt, String group,String service,String hostname,String metric,String status,String monHost, String tsMonitored, String tsProcessed, String prevStatus, String prevTs, String repeat, String summary, String message){
		this.report  = report;
		this.type =type;
		this.group = group;
		this.dt = dt;
		this.service = service;
		this.hostname = hostname;
		this.metric = metric;
		this.status = status;
		this.monHost = monHost;
		this.tsMonitored = tsMonitored;
		this.tsProcessed = tsProcessed;
		this.prevStatus = prevStatus;
		this.prevTs = prevTs;
		this.repeat = repeat;
		this.summary = summary;
		this.message = message;
		
	}
	
	public String getReport() { return report; }
	public String getType() { return type; }
	public String getDt() {return dt;}
	public String getGroup() {return group;}
	public String getService() {return service;}
	public String getHostname() {return hostname;}
	public String getStatus() {return status;}
	public String getMetric() {return metric;}
	public String getMonHost() {return monHost;}
	public String getTsMonitored() {return tsMonitored;}
	public String getTsProcessed() {return tsProcessed;}
	public String getPrevStatus() {return prevStatus;}
	public String getPrevTs() {return prevTs;}
	public String getRepeat() {return repeat;}
	public String getSummary() {return this.summary;}
	public String getMessage() {return this.message;}

	public void setReport(String report) {this.report = report;}	
	public void setType(String type) {this.type = type;}
	public void setDt(String dt) {this.dt = dt;}
	public void setGroup(String group) {this.group = group;}
	public void setService(String service) {this.service = service;}
	public void setHostname(String hostname) {this.hostname = hostname;}
	public void setMetric(String metric) {this.metric = metric;}
	public void setStatus(String status) {this.status = status;}
	public void setMonHost(String monHost) {this.monHost = monHost;}
	public void setTsMonitored(String tsMonitored) {this.tsMonitored = tsMonitored;}
	public void setTsProcessed(String tsProcessed) {this.tsProcessed = tsProcessed;}
	public void setPrevStatus(String prevStatus) {this.prevStatus = prevStatus;}
	public void setPrevTs(String prevTs) {this.prevTs = prevTs;}
	public void setRepeat(String repeat) {this.repeat = repeat;}
	public void setSummary(String summary) {this.summary = summary;}
	public void setMessage (String message) {this.message = message;}
	
	public int getDateInt() {
		return Integer.parseInt(this.dt);
	}
	
	public int getTimeInt() {
		String timePart = this.tsMonitored.replaceAll(":|Z", "").split("T")[1];
		return Integer.parseInt(timePart);
	}
	
	
}