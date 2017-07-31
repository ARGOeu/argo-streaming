package argo.batch;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.RichFlatMapFunction;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;
import argo.avro.MetricData;
import argo.avro.MetricProfile;
import ops.DIntegrator;
import ops.DTimeline;
import ops.OpsManager;
import sync.AggregationProfileManager;
import sync.EndpointGroupManager;
import sync.GroupGroupManager;
import sync.MetricProfileManager;
import sync.RecomputationManager;

/**
 * Accepts a service monitor timeline entry and produces a ServiceAR object by
 * calculating a/r over timeline data
 */
public class CalcServiceAR extends RichFlatMapFunction<MonTimeline, ServiceAR> {

	private static final long serialVersionUID = 1L;

	final ParameterTool params;

	public CalcServiceAR(ParameterTool params) {
		this.params = params;
	}

	static Logger LOG = LoggerFactory.getLogger(ArgoArBatch.class);

	private List<MetricProfile> mps;
	private List<GroupEndpoint> egp;
	private List<GroupGroup> ggp;
	private List<String> apr;
	private List<String> rec;
	private List<String> ops;
	private MetricProfileManager mpsMgr;
	private EndpointGroupManager egpMgr;
	private GroupGroupManager ggpMgr;
	private AggregationProfileManager aprMgr;
	private RecomputationManager recMgr;
	private OpsManager opsMgr;

	private String egroupType;
	private String runDate;
	private String report;

	/**
	 * Initialization method of the RichFlatMapFunction operator
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
	public void open(Configuration parameters) throws IOException, ParseException {
		// Get data from broadcast variable
		this.mps = getRuntimeContext().getBroadcastVariable("mps");
		this.egp = getRuntimeContext().getBroadcastVariable("egp");
		this.ggp = getRuntimeContext().getBroadcastVariable("ggp");
		this.apr = getRuntimeContext().getBroadcastVariable("apr");
		this.rec = getRuntimeContext().getBroadcastVariable("rec");
		this.ops = getRuntimeContext().getBroadcastVariable("ops");

		// Initialize metric profile manager
		this.mpsMgr = new MetricProfileManager();
		this.mpsMgr.loadFromList(mps);
		// Initialize endpoint group manager
		this.egpMgr = new EndpointGroupManager();
		this.egpMgr.loadFromList(egp);

		this.ggpMgr = new GroupGroupManager();
		this.ggpMgr.loadFromList(ggp);

		// Initialize Aggregation Profile Manager ;
		this.aprMgr = new AggregationProfileManager();
		this.aprMgr.loadJsonString(apr);

		// Initialize Recomputations Manager;
		this.recMgr = new RecomputationManager();
		this.recMgr.loadJsonString(rec);
		
		// Initialize Operations Manager;
		this.opsMgr = new OpsManager();
		this.opsMgr.loadJsonString(ops);

		// Initialize endpoint group type
		this.egroupType = params.getRequired("egroup.type");

		// Initialize rundate
		this.runDate = params.getRequired("run.date");

		// Initialize report
		this.report = params.getRequired("report");
	}

	/**
	 * The main operator business logic of calculating a/r results from timeline
	 * data
	 * <p>
	 * Uses a DIntegrator to scan the timeline and calculate availability and
	 * reliability scores
	 *
	 * @param in
	 *            A MonTimeline Object representing a service timeline
	 * @param out
	 *            A ServiceAR Object containing a/r results
	 */
	@Override
	public void flatMap(MonTimeline mtl, Collector<ServiceAR> out) throws Exception {
		
		
		
		DIntegrator dAR = new DIntegrator();
		dAR.calculateAR(mtl.getTimeline(),this.opsMgr); 
		
		int runDateInt = Integer.parseInt(this.runDate.replace("-", ""));
		
		ServiceAR result = new ServiceAR(runDateInt,this.report,mtl.getService(),mtl.getGroup(),dAR.availability,dAR.reliability,dAR.up_f,dAR.unknown_f,dAR.down_f);
		
		out.collect(result);
	
	}

}
