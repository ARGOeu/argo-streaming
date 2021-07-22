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
	private @SerializedName("monitoring_host") String monHost;
	private @SerializedName("ts_monitored") String tsMonitored;
	private @SerializedName("ts_processed") String tsProcessed;
	private String repeat;
	private String summary;
	private String message;
	
	private String status;
	private @SerializedName("prev_status") String prevStatus;
	private @SerializedName("prev_ts") String prevTs;
	
	// Record status changes from other layers
	// Arrays include 4 store elements in the following order [status, previous_status, timestamp, previous_timestamp]
	private @SerializedName("status_egroup") String statusEgroup[];
	private @SerializedName("status_service") String statusService[];
	private @SerializedName("status_endpoint") String statusEndpoint[];
	private @SerializedName("status_metric") String statusMetric[];
	// Record statuses of the other groups
	private @SerializedName("group_statuses") String groupStatuses[];
	private @SerializedName("group_endpoints") String groupEndpoints[];
	private @SerializedName("group_services") String groupServices[];
	
	// Record all statuses of endpoint's metrics
	private @SerializedName("metric_statuses") String metricStatuses[];
	private @SerializedName("metric_names") String metricNames[];
	
	
	public StatusEvent() {
		this.report  = "";
		this.type = "";
		this.group = "";
		this.dt = "";
		this.service = "";
		this.hostname = "";
		this.metric = "";
		this.status = "";
		this.monHost = "";
		this.tsMonitored = "";
		this.tsProcessed = "";
		this.prevStatus = "";
		this.prevTs = "";
		this.repeat = "";
		this.summary = "";
		this.message = "";
		this.statusEgroup = new String[0];
		this.statusService = new String[0];
		this.statusEndpoint = new String[0];
		this.statusMetric = new String[0];
		this.groupEndpoints = new String[0];
		this.groupServices= new String[0];
		this.groupStatuses = new String[0];
		this.metricStatuses = new String[0];
		this.metricNames = new String[0];
		
	}
	
	
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
		this.statusEgroup = null;
		this.statusService = null;
		this.statusEndpoint = null;
		this.statusMetric = null;
		this.groupEndpoints = null;
		this.groupServices = null;
		this.groupStatuses = null;
		this.metricStatuses = null;
		this.metricNames = null;

		
	}
	
	public String[] getStatusEgroup() {
		return this.statusEgroup;
	}
	
	public String[] getStatusService() {
		return this.statusService;
	}
	
	public String[] getStatusEndpoint() {
		return this.statusEndpoint;
	}
	
	public String[] getStatusMetric() {
		return this.statusMetric;
	}
	
	public void setStatusEgroup(String[] statusEgroup ) {
		this.statusEgroup = statusEgroup;
	}
	
	public void setStatusService(String[]  statusService ) {
		this.statusService = statusService;
	}
	public void setStatusEndpoint(String[]  statusEndpoint ) {
		this.statusEndpoint = statusEndpoint;
	}
	public void setStatusMetric(String[]  statusMetric ) {
		this.statusMetric = statusMetric;
	}
	
	public void setGroupStatuses(String[] groupStatuses) {
		this.groupStatuses = groupStatuses;
	}
	
	public void setGroupEndpoints(String[] groupEndpoints) {
		this.groupEndpoints = groupEndpoints;
	}
	
	
	public void setGroupServices(String[] groupServices) {
		this.groupServices = groupServices;
	}
	
	public String[] getGroupStatuses() {
		return this.groupStatuses;
	}
	
	public String[] getGroupServices() {
		return this.groupServices;
	}
	
	public String[] getGroupEndpoints() {
		return this.groupEndpoints;
	}
	
	public String[] getMetricStatuses() {
		return this.metricStatuses;
	}
	
	public String[] getMetricNames() {
		return this.metricNames;
	}
	

	public void setMetricNames(String[] metricNames) {
		this.metricNames = metricNames;
	}
	
	public void setMetricStatuses(String[] metricStatuses) {
		this.metricStatuses = metricStatuses;
	}
	
	
	public String getReport() { return report; }
	public String getType() { return type; }
	public String getDt() {return dt;}
	public String getGroup() {return group;}
	public String getService() {return service;}
	public String getHostname() {return hostname;}

	public String getMetric() {return metric;}
	public String getMonHost() {return monHost;}
	public String getTsMonitored() {return tsMonitored;}
	public String getTsProcessed() {return tsProcessed;}

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

	public void setMonHost(String monHost) {this.monHost = monHost;}
	public void setTsMonitored(String tsMonitored) {this.tsMonitored = tsMonitored;}
	public void setTsProcessed(String tsProcessed) {this.tsProcessed = tsProcessed;}
	public String getPrevStatus() {return prevStatus;}
	public String getPrevTs() {return prevTs;}
	public String getStatus() {return status;}
	public void setPrevStatus(String prevStatus) {this.prevStatus = prevStatus;}
	public void setPrevTs(String prevTs) {this.prevTs = prevTs;}
	public void setRepeat(String repeat) {this.repeat = repeat;}
	public void setStatus(String status) {this.status = status;}
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