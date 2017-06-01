package argo.batch;

import java.util.List;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;

import argo.avro.MetricProfile;
import sync.EndpointGroupManager;
import sync.GroupGroupManager;
import sync.MetricProfileManager;

public class CalcPrevStatus extends RichGroupReduceFunction<StatusMetric,StatusMetric> {

	private static final long serialVersionUID = 1L;

	
	final ParameterTool params;
	
	public CalcPrevStatus(ParameterTool params){
		this.params = params;
	}
	
	static Logger LOG = LoggerFactory.getLogger(ArgoStatusBatch.class);

	private List<MetricProfile> mps;
	private List<GroupEndpoint> egp;
	private List<GroupGroup> ggp;
	private MetricProfileManager mpsMgr;
	private EndpointGroupManager egpMgr;
	private GroupGroupManager ggpMgr;
	private String egroupType;
	private String runDate;

	@Override
	public void open(Configuration parameters) {
		// Get data from broadcast variable
		this.runDate = params.getRequired("run.date");
		
		this.mps = getRuntimeContext().getBroadcastVariable("mps");
		this.egp = getRuntimeContext().getBroadcastVariable("egp");
		this.ggp = getRuntimeContext().getBroadcastVariable("ggp");
		// Initialize metric profile manager
		this.mpsMgr = new MetricProfileManager();
		this.mpsMgr.loadFromList(mps);
		// Initialize endpoint group manager
		this.egpMgr = new EndpointGroupManager();
		this.egpMgr.loadFromList(egp);

		
		// Initialize group group manager
		this.ggpMgr = new GroupGroupManager();
		this.ggpMgr.loadFromList(ggp);
		// Initialize endpoint group type
		this.egroupType = params.get("egroup.type");
	}

	@Override
	public void reduce(Iterable<StatusMetric> in, Collector<StatusMetric> out) throws Exception {
		// group input is sorted 
		String prevStatus = "MISSING";
		String prevTimestamp = this.runDate+"T00:00:00Z";
		boolean gotPrev = false;
		for (StatusMetric item : in){
			// If haven't captured yet previous timestamp
			if (!gotPrev){
				if (item.getTimestamp().split("T")[0].compareToIgnoreCase(this.runDate) != 0) {
					// set prevTimestamp to this
					prevTimestamp = item.getTimestamp();
					prevStatus = item.getStatus();
					gotPrev = true;
					continue;
				}
			}
			
			item.setPrevState(prevStatus);
			item.setPrevTs(prevTimestamp);
			
			if (item.getTimestamp().split("T")[0].compareToIgnoreCase(this.runDate) == 0){
				out.collect(item);
			}
			
			
			
			
			prevStatus = item.getStatus();
			prevTimestamp = item.getTimestamp();
			
			
			
		}
		
	}

	


}
