package argo.streaming;

import java.io.Serializable;

import org.apache.flink.api.java.utils.ParameterTool;

public class StatusConfig implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public String runDate;
	
	// Ams parameters
	public String amsHost;
	public String amsPort;
	public String amsToken; 
	public String amsProject;
	public String amsSub;
	
	// Avro schema
	public String avroSchema;
	
	public String report;
	
	public String apiEndpoint;
	public String apiToken;
	public String apiProxy;
	public Boolean apiVerify = false;
	public int hourCheck = 24;
	public String reportID;
	public long interval = 100L;
	
	// Sync files
	public String aps;
	public String mps;
	public String egp;
	public String ops;
	public String downtime;
	// Parameter used in alert timeouts for notifications
	public long timeout;
	// Parameter used for daily event generation (not used in notifications)
	public boolean daily;
	// Parameter used to initialize a status to a default value (OK optimistically, MISSING pessimistically)
	public String initStatus;
	
	// Raw parameters
	public final ParameterTool pt;
	
	public StatusConfig(ParameterTool pt){
	   this.pt = pt;
	   this.amsHost = pt.getRequired("ams.endpoint");
	   this.amsPort = pt.getRequired("ams.port");
	   this.amsToken = pt.getRequired("ams.token");
	   this.amsProject = pt.getRequired("ams.project");
	   this.apiEndpoint = pt.getRequired("api.endpoint");
	   
	   
	   this.aps = pt.getRequired("sync.apr");
	   this.mps = pt.getRequired("sync.mps");
	   this.egp = pt.getRequired("sync.egp");
	   this.ops = pt.getRequired("sync.ops");
	   this.runDate = pt.getRequired("run.date");
	   this.downtime = pt.getRequired("sync.downtime");
	   this.report = pt.getRequired("report");
	   // Optional timeout parameter
	   if (pt.has("timeout")){
		   this.timeout = pt.getLong("timeout");
	   } else {
		   this.timeout = 86400000L;
	   }
	   
	   // optional cli parameter to configure default status
	   if (pt.has("init.status")) {
		   this.initStatus = pt.get("init.status");
	   } else {
		   // by default, default initial status should be optimistically OK
		   this.initStatus = "OK";
	   }
	   
	   // Optional set daily parameter
	   
	   this.apiEndpoint = pt.getRequired("api.endpoint");
	   this.apiToken = pt.getRequired("apiToken");
	   this.reportID = pt.getRequired("reportID");
	   
	   if (pt.has("api.proxy")) this.apiProxy = pt.get("api.proxy","");
	   if (pt.has("api.verify")) this.apiVerify = pt.getBoolean("api.verify",false);
	   if (pt.has("api.interval")) this.hourCheck = pt.getInt("api.interval",24);
	   if (pt.has("ams.interval")) this.interval = pt.getLong("interval",100L);
	   
	   this.daily = pt.getBoolean("daily",false);
	   
	  }
	
	public ParameterTool getParameters(){
	    return this.pt;
	  }

	
}
