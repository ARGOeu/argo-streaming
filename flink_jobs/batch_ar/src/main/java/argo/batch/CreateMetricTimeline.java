package argo.batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
 * Accepts a list of metric data objects and produces a metric timeline
 * The class is used as a RichGroupReduce Function in flink pipeline
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

	

	/**
	 * Initialization method of the RichGroupReduceFunction operator
	 * <p>
	 * This runs at the initialization of the operator and receives a configuration
	 * parameter object. It initializes all required structures used by this operator
	 * such as profile managers, operations managers, topology managers etc.
	 *
	 * @param	parameters	A flink Configuration object
	 */
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

	

	}

	/**
	 * The main operator buisness logic of transforming a collection of MetricData to a metric timeline
	 * <p>
	 * This runs for each group item (endpointGroup,service,hostname,metric) and contains a list of metric data objects sorted
	 * by the "timestamp" field. It uses a Discrete Timeline object to map individual status change points in time to an array of statuses.
	 * The Discrete timeline automatically fills the gaps between the status changes to produce a full array of status points representing
	 * the discrete timeline. Notice that status values are mapped from string representations to integer ids ("OK" => 0, "CRITICAL" => 4)
	 * for more efficient processing during aggregation comparisons.
	 *
	 * @param	in	An Iterable collection of MetricData objects
	 * @param	out	A Collector list of MonTimeline to acquire the produced metric timelines. 
	 */
	@Override
	public void reduce(Iterable<MetricData> in, Collector<MonTimeline> out) throws Exception {

		String service = "";
		String endpointGroup = "";
		String hostname = "";
		String metric = "";

		DTimeline dtl = new DTimeline();

		for (MetricData item : in) {

			service = item.getService();
			hostname = item.getHostname();
			metric = item.getMetric();

			// Filter By endpoint group if belongs to supergroup
			ArrayList<String> groupnames = egpMgr.getGroup(egroupType, hostname, service);

			for (String groupname : groupnames) {
				if (ggpMgr.checkSubGroup(groupname) == true) {
					endpointGroup = groupname;
				}

			}

			String ts = item.getTimestamp();
			String status = item.getStatus();
			// insert monitoring point (ts,status) into the discrete timeline
			
			if (!(ts.substring(0, ts.indexOf("T")).equals(this.runDate))) {
				dtl.setStartState(this.opsMgr.getIntStatus(status));
				continue;
			}
			
			dtl.insert(ts, opsMgr.getIntStatus(status));

		}
		
		dtl.settle(opsMgr.getDefaultMissingInt());
		
		// Create a new MonTimeline object
		MonTimeline mtl = new MonTimeline(endpointGroup, service, hostname, metric);
		// Add Discrete Timeline samples int array to the MonTimeline
		mtl.setTimeline(dtl.samples);
		// Output MonTimeline object
		out.collect(mtl);

	}

}
