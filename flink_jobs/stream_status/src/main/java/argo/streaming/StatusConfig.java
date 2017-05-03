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
	
	// Raw parameters
	public final ParameterTool pt;
	
	public StatusConfig(ParameterTool pt){
	    this.pt = pt;
	   this.amsHost = pt.get("ams.host","localhost");
	   this.amsPort = pt.get("ams.port","443");
	   this.amsToken = pt.get("ams.token","metric");
	   this.amsProject = pt.get("ams.project","TESTPROJECT");
	   this.avroSchema = pt.get("avro.schema","metric_data.avsc");
	   this.aps = pt.get("sync.aps","ap1.json");
	   this.mps = pt.get("sync.mps","metric_data.avro");
	   this.egp = pt.get("sync.egp","group_endpoints.avro");
	   this.ops = pt.get("sync.ops","ops.json");
	   this.runDate = pt.get("run.date","");
	   
	  }
	
	public ParameterTool getParameters(){
	    return this.pt;
	  }

	
}
