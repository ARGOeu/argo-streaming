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

import argo.avro.Downtime;
import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;

import argo.avro.MetricProfile;
import ops.DAggregator;
import ops.DTimeline;
import ops.OpsManager;
import sync.AggregationProfileManager;
import sync.DowntimeManager;
import sync.EndpointGroupManager;
import sync.GroupGroupManager;
import sync.MetricProfileManager;

/**
 * Accepts a list of monitoring timelines and produces an endpoint timeline The
 * class is used as a RichGroupReduce Function in flink pipeline
 */
public class CreateEndpointTimeline extends RichGroupReduceFunction<MonTimeline, MonTimeline> {

	private static final long serialVersionUID = 1L;

	final ParameterTool params;

	public CreateEndpointTimeline(ParameterTool params) {
		this.params = params;
	}

	static Logger LOG = LoggerFactory.getLogger(ArgoArBatch.class);

	private List<MetricProfile> mps;
	private List<String> aps;
	private List<String> ops;
	private List<GroupEndpoint> egp;
	private List<GroupGroup> ggp;
	private List<Downtime> downtime;
	private MetricProfileManager mpsMgr;
	private AggregationProfileManager apsMgr;
	private EndpointGroupManager egpMgr;
	private GroupGroupManager ggpMgr;
	private OpsManager opsMgr;
	private DowntimeManager downtimeMgr;
	private String runDate;

	/**
	 * Initialization method of the RichGroupReduceFunction operator
	 * <p>
	 * This runs at the initialization of the operator and receives a
	 * configuration parameter object. It initializes all required structures
	 * used by this operator such as profile managers, operations managers,
	 * topology managers etc.
	 *
	 * @param parameters
	 *            A flink Configuration object
	 */
	@Override
	public void open(Configuration parameters) throws IOException {

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

		// Initialize downtime manager
		this.downtimeMgr = new DowntimeManager();
		this.downtimeMgr.loadFromList(downtime);

		this.runDate = params.getRequired("run.date");

	}

	/**
	 * The main operator business logic of transforming a collection of
	 * MetricTimelines to an aggregated endpoint timeline
	 * <p>
	 * This runs for each group item (endpointGroup,service,hostname) and
	 * contains a list of metric timelines sorted by the "metric" field. It uses
	 * a Discrete Aggregator to aggregate the metric timelines according to the
	 * operations profile defined in the Operations Manager as to produce the
	 * final Endpoint Timeline. The type of metric aggregation is defined in the
	 * aggregation profile managed by the AggregationManager
	 *
	 * @param in
	 *            An Iterable collection of MonTimeline objects
	 * @param out
	 *            A Collector list of MonTimeline to acquire the produced
	 *            endpoint timelines.
	 */
	@Override
	public void reduce(Iterable<MonTimeline> in, Collector<MonTimeline> out) throws Exception {

		// Initialize field values and aggregator
		String service = "";
		String endpointGroup = "";
		String hostname = "";

		DAggregator dAgg = new DAggregator();

		// For each metric timeline of the input group
		for (MonTimeline item : in) {

			service = item.getService();
			hostname = item.getHostname();
			endpointGroup = item.getGroup();
			// Initialize a DTimelineObject
			DTimeline dtl = new DTimeline();
			dtl.samples = item.getTimeline();
			dtl.setStartState(dtl.samples[0]);
			// Push Discrete Timeline directly to the hashtable of the
			// aggregator
			dAgg.timelines.put(item.getMetric(), dtl);

		}

		// Apply Downtimes if hostname is on downtime list
		ArrayList<String> downPeriod = this.downtimeMgr.getPeriod(hostname, service);

		if (downPeriod != null) {
			// We have downtime declared
			dAgg.aggregation.fill(this.opsMgr.getDefaultDownInt(), downPeriod.get(0), downPeriod.get(1),
					this.runDate);
		}

		// Grab metric operation type from aggregation profile
		String avProf = apsMgr.getAvProfiles().get(0);
		dAgg.aggregate(apsMgr.getMetricOp(avProf), opsMgr);
		// Create a new MonTimeline object for endpoint
		MonTimeline mtl = new MonTimeline(endpointGroup, service, hostname, "");
		// Add Discrete Timeline samples int array to the MonTimeline
		mtl.setTimeline(dAgg.aggregation.samples);
		// Output MonTimeline object
		out.collect(mtl);

	}

}