package argo.batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;

import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;
import argo.avro.MetricData;
import argo.avro.MetricProfile;
import ops.DTimeline;
import ops.OpsManager;
import sync.AggregationProfileManager;
import sync.EndpointGroupManager;
import sync.GroupGroupManager;
import sync.MetricProfileManager;

/**
 * Accepts a list of metric data grouped by the fields: endpoint group, service,
 * endpoint, metric and produces a mon timeline
 */
public class CreateMetricTimeline extends RichGroupReduceFunction<MetricData, MonTimeline> {

	private static final long serialVersionUID = 1L;

	final ParameterTool params;

	public CreateMetricTimeline(ParameterTool params) {
		this.params = params;
	}

	static Logger LOG = LoggerFactory.getLogger(ArgoArBatch.class);

	private List<MetricProfile> mps;
	private List<String> aps;
	private List<String> ops;
	private List<GroupEndpoint> egp;
	private List<GroupGroup> ggp;
	private MetricProfileManager mpsMgr;
	private AggregationProfileManager apsMgr;
	private EndpointGroupManager egpMgr;
	private GroupGroupManager ggpMgr;
	private OpsManager opsMgr;
	private String runDate;
	private String egroupType;

	private boolean fillMissing;

	@Override
	public void open(Configuration parameters) throws IOException {

		this.runDate = params.getRequired("run.date");
		// Get data from broadcast variables
		this.mps = getRuntimeContext().getBroadcastVariable("mps");
		this.aps = getRuntimeContext().getBroadcastVariable("aps");
		this.ops = getRuntimeContext().getBroadcastVariable("ops");
		this.egp = getRuntimeContext().getBroadcastVariable("egp");
		this.ggp = getRuntimeContext().getBroadcastVariable("ggp");
		// Initialize metric profile manager
		this.mpsMgr = new MetricProfileManager();
		this.mpsMgr.loadFromList(mps);
		// Initialize aggregation profile manager
		this.apsMgr = new AggregationProfileManager();

		this.apsMgr.loadJsonString(aps);
		// Initialize operations manager
		this.opsMgr = new OpsManager();
		this.opsMgr.loadJsonString(ops);

		// Initialize endpoint group manager
		this.egpMgr = new EndpointGroupManager();
		this.egpMgr.loadFromList(egp);

		this.ggpMgr = new GroupGroupManager();
		this.ggpMgr.loadFromList(ggp);

		this.runDate = params.getRequired("run.date");
		this.egroupType = params.getRequired("egroup.type");

		fillMissing = true;

	}

	@Override
	public void reduce(Iterable<MetricData> in, Collector<MonTimeline> out) throws Exception {

	

		
		String prevMetricName = "";

		// Only 1 profile per job
		String mProfile = this.mpsMgr.getProfiles().get(0);
		// Get default missing state
		int defMissing = this.opsMgr.getDefaultMissingInt();
		// Iterate all metric names of profile and initiate timelines

		String aprofile = this.apsMgr.getAvProfiles().get(0);

		String service ="";
		String endpointGroup ="";
		String hostname ="";
		String metric="";
		
		
		DTimeline dtl = new DTimeline();
		
		
		
		for (MetricData item : in) {
			if (fillMissing) {
				// Before reading metric messages, init expected metric
				// timelines

				service = item.getService();
				hostname = item.getHostname();
				metric = item.getMetric();

				// Filter By endpoint group if belongs to supergroup
				ArrayList<String> groupnames = egpMgr.getGroup(egroupType, hostname, service);
				
				for (String groupname : groupnames) {
					if (ggpMgr.checkSubGroup(groupname) == true){
						endpointGroup = groupname;
					}
						
						
				}
				this.mpsMgr.getProfileServiceMetrics(mProfile, item.getService());
				fillMissing = false;
			}

			service = item.getService();
			hostname = item.getHostname();
			metric = item.getMetric();
			String ts = item.getTimestamp();
			String status = item.getStatus();
			// insert monitoring point (ts,status) into the discrete timeline
			dtl.insert(ts, opsMgr.getIntStatus(status));
			

			

		}
		// Create a new MonTimeline object
		MonTimeline mtl = new MonTimeline(endpointGroup,service,hostname,metric);
		// Add Discrete Timeline samples int array to the MonTimeline 
		mtl.setTimeline(dtl.samples);
		// Output MonTimeline object
		out.collect(mtl);

	}

}
