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
	
	// Sync files
	public String aps;
	public String mps;
	public String egp;
	public String ops;
	// Parameter used in alert timeouts for notifications
	public long timeout;
	// Parameter used for daily event generation (not used in notifications)
	public boolean daily;
	public String defStatus = "MISSING";
	
	// Raw parameters
	public final ParameterTool pt;
	
	public StatusConfig(ParameterTool pt){
	   this.pt = pt;
	   this.amsHost = pt.getRequired("ams.endpoint");
	   this.amsPort = pt.getRequired("ams.port");
	   this.amsToken = pt.getRequired("ams.token");
	   this.amsProject = pt.getRequired("ams.project");
	   
	   this.aps = pt.getRequired("sync.apr");
	   this.mps = pt.getRequired("sync.mps");
	   this.egp = pt.getRequired("sync.egp");
	   this.ops = pt.getRequired("sync.ops");
	   this.runDate = pt.getRequired("run.date");
	   // Optional timeout parameter
	   if (pt.has("timeout")){
		   this.timeout = pt.getLong("timeout");
	   } else {
		   this.timeout = 86400000L;
	   }
	   
	   // Optional set daily parameter
	   this.daily = pt.getBoolean("daily",false);
	   
	  }
	
	public ParameterTool getParameters(){
	    return this.pt;
	  }

	
}
