package argo.batch;

import java.io.IOException;
import java.text.ParseException;
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
import ops.OpsManager;
import sync.AggregationProfileManager;
import sync.EndpointGroupManager;
import sync.GroupGroupManager;
import sync.MetricProfileManager;
import sync.RecomputationsManager;

/**
 * Accepts a metric data entry and converts it to a status metric object by appending endpoint group information
 * Filters out entries that do not appear in topology and metric profiles
 */
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
	private List<String> rec;
	private MetricProfileManager mpsMgr;
	private EndpointGroupManager egpMgr;
	private GroupGroupManager ggpMgr;
	private RecomputationsManager recMgr;
	
	private String egroupType;

	@Override
	public void open(Configuration parameters) throws IOException, ParseException {
		// Get data from broadcast variable
		this.mps = getRuntimeContext().getBroadcastVariable("mps");
		this.egp = getRuntimeContext().getBroadcastVariable("egp");
		this.ggp = getRuntimeContext().getBroadcastVariable("ggp");
		this.ggp = getRuntimeContext().getBroadcastVariable("ggp");
		this.rec = getRuntimeContext().getBroadcastVariable("rec");
		
		// Initialize Recomputation manager
		this.recMgr = new RecomputationsManager();
		this.recMgr.loadJsonString(rec);
		
		// Initialize metric profile manager
		this.mpsMgr = new MetricProfileManager();
		this.mpsMgr.loadFromList(mps);
		// Initialize endpoint group manager
		this.egpMgr = new EndpointGroupManager();
		this.egpMgr.loadFromList(egp);
		
		this.ggpMgr = new GroupGroupManager();
		this.ggpMgr.loadFromList(ggp);
		// Initialize endpoint group type
		this.egroupType = params.getRequired("egroup.type");
	}

	

	@Override
	public void flatMap(MetricData md, Collector<StatusMetric> out) throws Exception {

		String prof = mpsMgr.getProfiles().get(0);
		String hostname = md.getHostname();
		String service = md.getService();
		String metric = md.getMetric();
		String monHost = md.getMonitoringHost();
		String ts = md.getTimestamp();

		// Filter By monitoring engine
		if (recMgr.isMonExcluded(monHost, ts) == true) return;
		
		
		// Filter By metric profile
		if (mpsMgr.checkProfileServiceMetric(prof, service, metric) == false) return;

		
		// Filter By endpoint group if belongs to supergroup
		ArrayList<String> groupnames = egpMgr.getGroup(egroupType, hostname, service);
		
		for (String groupname : groupnames) {
			if (ggpMgr.checkSubGroup(groupname) == true){
				// Create a StatusMetric output
				String timestamp2 = md.getTimestamp().split("Z")[0];
				String[] tsToken = timestamp2.split("T");
				int dateInt = Integer.parseInt(tsToken[0].replace("-", ""));
				int timeInt = Integer.parseInt(tsToken[1].replace(":",""));
				
				StatusMetric sm = new StatusMetric(groupname,md.getService(),md.getHostname(),md.getMetric(), md.getStatus(),md.getTimestamp(),dateInt,timeInt,"","");
				
				out.collect(sm);
			}
				
				
		}
		
	}
}
