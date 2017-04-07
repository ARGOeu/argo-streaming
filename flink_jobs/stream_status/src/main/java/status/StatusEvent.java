package status;

import com.google.gson.annotations.SerializedName;

public class StatusEvent{
	String report;
	String type;
	@SerializedName("date") String dt;
	@SerializedName("endpoint_group") String group;
	String service;
	String hostname;
	String metric;
	String status;
	@SerializedName("monitoring_host") String monHost;
	@SerializedName("ts_monitored") String tsMonitored;
	@SerializedName("ts_processed") String tsProcessed;
	@SerializedName("prev_status")String prevStatus;
	@SerializedName("prev_ts")String prevTs;
	
	public StatusEvent (String report, String type, String dt, String group,String service,String hostname,String metric,String status,String monHost, String tsMonitored, String tsProcessed, String prevStatus, String prevTs){
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
	}
}