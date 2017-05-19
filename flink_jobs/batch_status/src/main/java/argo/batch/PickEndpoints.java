package argo.batch;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;
import argo.avro.MetricData;
import argo.avro.MetricProfile;
import sync.EndpointGroupManager;
import sync.GroupGroupManager;
import sync.MetricProfileManager;

public class PickEndpoints extends RichFlatMapFunction<MetricData,StatusMetric> {

	private static final long serialVersionUID = 1L;

	
	final ParameterTool params;
	
	public PickEndpoints(ParameterTool params){
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

	@Override
	public void open(Configuration parameters) {
		// Get data from broadcast variable
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
		this.egroupType = params.get("egroup-type");
	}

	

	@Override
	public void flatMap(MetricData md, Collector<StatusMetric> out) throws Exception {

		String prof = mpsMgr.getProfiles().get(0);
		String hostname = md.getHostname();
		String service = md.getService();
		String metric = md.getMetric();

		// Filter By metric profile
		if (mpsMgr.checkProfileServiceMetric(prof, service, metric) == false) return;

		
		// Filter By endpoint group if belongs to supergroup
		ArrayList<String> groupnames = egpMgr.getGroup(egroupType, hostname, service);
		
		for (String groupname : groupnames) {
			if (ggpMgr.checkSubGroup(groupname) == true){
				// Create a StatusMetric output
				String timestamp = md.getTimestamp().split("Z")[0];
				String[] ts = timestamp.split("T");
				int dateInt = Integer.parseInt(ts[0].replace("-", ""));
				int timeInt = Integer.parseInt(ts[1].replace(":",""));
				
				StatusMetric sm = new StatusMetric(groupname,md.getService(),md.getHostname(),md.getMetric(), md.getStatus(),md.getTimestamp(),dateInt,timeInt,"","");
				
				out.collect(sm);
			}
				
				
		}
		
	}
}
