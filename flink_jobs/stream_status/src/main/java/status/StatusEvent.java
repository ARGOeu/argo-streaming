package status;

import com.google.gson.annotations.SerializedName;

public class StatusEvent{
	String type;
	@SerializedName("endpoint_group") String group;
	String service;
	String hostname;
	String metric;
	String status;
	String timestamp;
	
	public StatusEvent (String type,String group,String service,String hostname,String metric,String status,String timestamp){
		this.type =type;
		this.group = group;
		this.service = service;
		this.hostname = hostname;
		this.metric = metric;
		this.status = status;
		this.timestamp = timestamp;
	}
}