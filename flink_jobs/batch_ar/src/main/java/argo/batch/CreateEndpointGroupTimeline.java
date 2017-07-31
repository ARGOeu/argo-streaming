package argo.batch;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;

import argo.avro.MetricProfile;
import ops.DAggregator;
import ops.DTimeline;
import ops.OpsManager;
import sync.AggregationProfileManager;
import sync.EndpointGroupManager;
import sync.GroupGroupManager;
import sync.MetricProfileManager;
import sync.RecomputationManager;

/**
 * Accepts a list of service timelines and produces an aggregated endpoint group
 * timeline The class is used as a RichGroupReduce Function in flink pipeline
 */
public class CreateEndpointGroupTimeline extends RichGroupReduceFunction<MonTimeline, MonTimeline> {

	private static final long serialVersionUID = 1L;

	final ParameterTool params;

	public CreateEndpointGroupTimeline(ParameterTool params) {
		this.params = params;
	}

	static Logger LOG = LoggerFactory.getLogger(ArgoArBatch.class);

	private List<MetricProfile> mps;
	private List<String> aps;
	private List<String> ops;
	private List<GroupEndpoint> egp;
	private List<GroupGroup> ggp;
	private List<String> rec;
	private MetricProfileManager mpsMgr;
	private AggregationProfileManager apsMgr;
	private EndpointGroupManager egpMgr;
	private GroupGroupManager ggpMgr;
	private OpsManager opsMgr;
	private RecomputationManager recMgr;
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
	 * @throws ParseException 
	 */
	@Override
	public void open(Configuration parameters) throws IOException, ParseException {

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

		// Initialize Recomputations Manager;
		this.recMgr = new RecomputationManager();
		this.recMgr.loadJsonString(rec);
		
		this.runDate = params.getRequired("run.date");

	}

	/**
	 * The main operator business logic of transforming a collection of service
	 * timelines to an aggregated endpoint group timeline
	 * <p>
	 * This runs for each group item (endpointGroup) and contains a list of
	 * service timelines sorted by the "service" field. It uses multiple
	 * Discrete Aggregator to aggregate the endpoint timelines according to the
	 * aggregation groups defined in the aggregation profile and the operations
	 * defined in the operations profile to produce the final endpoint group
	 * timeline.
	 *
	 * @param in
	 *            An Iterable collection of MonTimeline objects
	 * @param out
	 *            A Collector list of MonTimeline to acquire the produced group
	 *            endpoint timelines.
	 */
	@Override
	public void reduce(Iterable<MonTimeline> in, Collector<MonTimeline> out) throws Exception {

		// Initialize field values and aggregator
		String service = "";
		String endpointGroup = "";

		// Create a Hasmap of Discrete Aggregators for each aggregation group
		// based on aggregation profile
		Map<String, DAggregator> groupAggr = new HashMap<String, DAggregator>();

		// Grab metric operation type from aggregation profile
		String avProf = apsMgr.getAvProfiles().get(0);
		// For each service timeline of the input group
		for (MonTimeline item : in) {

			service = item.getService();
			endpointGroup = item.getGroup();

			// Get the aggregation group
			String group = apsMgr.getGroupByService(avProf, service);

			// if group doesn't exist yet create it
			if (groupAggr.containsKey(group) == false) {
				groupAggr.put(group, new DAggregator());
			}

			// Initialize a DTimelineObject
			DTimeline dtl = new DTimeline();
			dtl.samples = item.getTimeline();
			dtl.setStartState(dtl.samples[0]);

			// group will be present now
			groupAggr.get(group).timelines.put(service, dtl);
		}

		// Aggregate each group
		for (String group : groupAggr.keySet()) {
			// Get group Operation

			String gop = this.apsMgr.getProfileGroupOp(avProf, group);

			groupAggr.get(group).aggregate(gop, this.opsMgr);

		}

		// Combine group aggregates to a final endpoint group aggregation
		// Aggregate all sites
		DAggregator totalSite = new DAggregator();

		// Aggregate each group
		for (String group : groupAggr.keySet()) {
			DTimeline curTimeline = groupAggr.get(group).aggregation;
			for (int i = 0; i < curTimeline.samples.length; i++) {
				totalSite.insertSlot(group, i, curTimeline.samples[i]);

			}

		}

		// Final site aggregate
		// Get appropriate operation from availability profile
		totalSite.aggregate(this.apsMgr.getTotalOp(avProf), this.opsMgr);

		// Check if endpoint group is excluded in recomputations
		if (this.recMgr.isExcluded(endpointGroup)) {

			ArrayList<Map<String, String>> periods = this.recMgr.getPeriods(endpointGroup, this.runDate);

			for (Map<String, String> period : periods) {
				totalSite.aggregation.fill(this.opsMgr.getDefaultUnknownInt(), period.get("start"), period.get("end"),
						this.runDate);
			}

		}

		// Create a new MonTimeline object for endpoint group
		MonTimeline mtl = new MonTimeline(endpointGroup, "", "", "");
		// Add Discrete Timeline samples int array to the MonTimeline
		mtl.setTimeline(totalSite.aggregation.samples);
		// Output MonTimeline object
		out.collect(mtl);

	}

}