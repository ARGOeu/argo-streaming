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

import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;

import argo.avro.MetricProfile;
import ops.CAggregator;
import ops.OpsManager;
import sync.AggregationProfileManager;
import sync.GroupGroupManager;
import sync.MetricProfileManager;


/**
 * Accepts a list o status metrics grouped by the fields: endpoint group, service, endpoint
 * Uses Continuous Timelines and Aggregators to calculate the status results of a service endpoint
 * Prepares the data in a form aligned with the datastore schema for status endpoint collection
 */
public class CalcStatusEndpoint extends RichGroupReduceFunction<StatusMetric, StatusMetric> {

	private static final long serialVersionUID = 1L;

	final ParameterTool params;

	public CalcStatusEndpoint(ParameterTool params) {
		this.params = params;
	}

	static Logger LOG = LoggerFactory.getLogger(ArgoStatusBatch.class);

	private List<MetricProfile> mps;
	private List<GroupEndpoint> egp;
	private List<GroupGroup> ggp;
	private List<String> aps;
	private List<String> ops;
	private MetricProfileManager mpsMgr;
	private AggregationProfileManager apsMgr;
	private OpsManager opsMgr;
	private GroupGroupManager ggpMgr;
	private String egroupType;
	private String runDate;
	private CAggregator endpointAggr;

	private boolean fillMissing;

	@Override
	public void open(Configuration parameters) throws IOException {
		// Get data from broadcast variable
		this.runDate = params.getRequired("run.date");

		this.mps = getRuntimeContext().getBroadcastVariable("mps");
		this.egp = getRuntimeContext().getBroadcastVariable("egp");
		this.ggp = getRuntimeContext().getBroadcastVariable("ggp");
		this.aps = getRuntimeContext().getBroadcastVariable("aps");
		this.ops = getRuntimeContext().getBroadcastVariable("ops");
		// Initialize metric profile manager
		this.mpsMgr = new MetricProfileManager();
		this.mpsMgr.loadFromList(mps);
		// Initialize aggregation profile manager
		this.apsMgr = new AggregationProfileManager();
		
		this.apsMgr.loadJsonString(aps);
		// Initialize operations manager
		this.opsMgr = new OpsManager();
		this.opsMgr.loadJsonString(ops);
		// Initialize group group manager

		// Initialize group group manager
		this.ggpMgr = new GroupGroupManager();
		this.ggpMgr.loadFromList(ggp);
		// Initialize endpoint group type
		this.egroupType = params.getRequired("egroup.type");
		this.runDate = params.getRequired("run.date");
		this.endpointAggr = new CAggregator(); // Create aggregator

		this.fillMissing = true;
	}

	@Override
	public void reduce(Iterable<StatusMetric> in, Collector<StatusMetric> out) throws Exception {

		this.endpointAggr.clear();

		String defTimestamp = this.endpointAggr.tsFromDate(this.runDate);
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
		int dateInt = Integer.parseInt(this.runDate.replace("-", ""));

		
		
		for (StatusMetric item : in) {
			if (fillMissing) {
				// Before reading metric messages, init expected metric
				// timelines

				service = item.getService();
				endpointGroup = item.getGroup();
				hostname = item.getHostname();


				this.mpsMgr.getProfileServiceMetrics(mProfile, item.getService());

				
				for (String mName : this.mpsMgr.getProfileServiceMetrics(mProfile, service)) {
					this.endpointAggr.createTimeline(mName, defTimestamp, defMissing);
				}

				fillMissing = false;
			}

			service = item.getService();
			endpointGroup = item.getGroup();
			hostname = item.getHostname();
			String metric = item.getMetric();
			String ts = item.getTimestamp();
			String status = item.getStatus();
			String prevStatus = item.getPrevState();
			

			// Check if we are in the switch of a new metric name
			if (prevMetricName.equals(metric) == false) {
				LOG.info("--another metric-- " + endpointGroup+"|"+service+"|"+hostname+"|"+metric+"|"+status+"|"+ts);
				this.endpointAggr.setFirst(metric, ts, this.opsMgr.getIntStatus(prevStatus));
				prevMetricName = metric;
				continue;
			}
			LOG.info("--same metric-- " + endpointGroup+"|"+service+"|"+hostname+"|"+metric+"|"+status+"|"+ts);
			this.endpointAggr.insert(metric, ts, this.opsMgr.getIntStatus(status));
			prevMetricName = metric;

		}

		this.endpointAggr.aggregate(this.opsMgr, this.apsMgr.getMetricOp(aprofile));

		// Append the timeline
		
		
		
		for (Entry<DateTime, Integer> item : this.endpointAggr.getSamples()) {
			
			StatusMetric cur = new StatusMetric();
			cur.setDateInt(dateInt);
			cur.setGroup(endpointGroup);
			cur.setHostname(hostname);
			cur.setService(service);
			
			
			cur.setTimestamp(item.getKey().toString(DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")));
			cur.setStatus(opsMgr.getStrStatus(item.getValue()));
			out.collect(cur);
		}

	}

}
