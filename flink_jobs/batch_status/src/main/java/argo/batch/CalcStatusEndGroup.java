package argo.batch;

import java.io.IOException;
import java.util.HashMap;
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

import ops.CAggregator;
import ops.OpsManager;
import sync.AggregationProfileManager;

/**
 * Accepts a list o status metrics grouped by the fields: endpoint group
 * Uses Continuous Timelines and Aggregators to calculate the status results of an endpoint group
 * Prepares the data in a form aligned with the datastore schema for status flavor collection
 */
public class CalcStatusEndGroup extends RichGroupReduceFunction<StatusMetric, StatusMetric> {

	private static final long serialVersionUID = 1L;

	final ParameterTool params;

	public CalcStatusEndGroup(ParameterTool params) {
		this.params = params;
	}

	static Logger LOG = LoggerFactory.getLogger(ArgoStatusBatch.class);

	
	
	private List<String> aps;
	private List<String> ops;
	
	private AggregationProfileManager apsMgr;
	private OpsManager opsMgr;
	

	private String runDate;
	public HashMap<String, CAggregator> groupEndpointAggr;

	private boolean getGroup;
	
	@Override
	public void open(Configuration parameters) throws IOException {
		
		this.runDate = params.getRequired("run.date");
		// Get data from broadcast variables
		this.aps = getRuntimeContext().getBroadcastVariable("aps");
		this.ops = getRuntimeContext().getBroadcastVariable("ops");
		
		// Initialize aggregation profile manager
		this.apsMgr = new AggregationProfileManager();
		this.apsMgr.loadJsonString(aps);
		// Initialize operations manager
		this.opsMgr = new OpsManager();
		this.opsMgr.loadJsonString(ops);
	
		// Initialize endpoint group type
		this.runDate = params.getRequired("run.date");
		// set the Structures
		this.groupEndpointAggr = new HashMap<String, CAggregator>();

		this.getGroup = true;
	}

	@Override
	public void reduce(Iterable<StatusMetric> in, Collector<StatusMetric> out) throws Exception {

		this.groupEndpointAggr.clear();

	
		
		String aProfile = this.apsMgr.getAvProfiles().get(0);
		String service ="";
		String endpointGroup ="";
		int dateInt = Integer.parseInt(this.runDate.replace("-", ""));

		
		
		for (StatusMetric item : in) {
			
			if (getGroup){
				endpointGroup = item.getGroup();
				getGroup =false;
			}
			
			service = item.getService();
			endpointGroup = item.getGroup();
		
			String ts = item.getTimestamp();
			String status = item.getStatus();
		
			
			// Get the availability group
			String group = apsMgr.getGroupByService(aProfile, service);
			
			// if group doesn't exist yet create it
			if (this.groupEndpointAggr.containsKey(group) == false) {
				this.groupEndpointAggr.put(group, new CAggregator());
			}
			
			this.groupEndpointAggr.get(group).insert(service, ts, this.opsMgr.getIntStatus(status));

			

		}

		// Aggregate each group
		for (String group : this.groupEndpointAggr.keySet()) {
			// Get group Operation

			String gop = this.apsMgr.getProfileGroupOp(aProfile, group);

			this.groupEndpointAggr.get(group).aggregate(this.opsMgr, gop);

		}
		
		// Aggregate all sites
		CAggregator totalSite = new CAggregator();

		// Aggregate each group
		for (String group : this.groupEndpointAggr.keySet()) {
			for (Entry<DateTime,Integer> item : this.groupEndpointAggr.get(group).getSamples()) {
				String ts = item.getKey().toString(DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"));
				totalSite.insert(group,ts, item.getValue());
			}

		}

		totalSite.aggregate( this.opsMgr,apsMgr.getTotalOp(aProfile));

		// Append the timeline	
		for (Entry<DateTime, Integer> item : totalSite.getSamples()) {
			
			StatusMetric cur = new StatusMetric();
			cur.setDateInt(dateInt);
			cur.setGroup(endpointGroup);
			
			
			cur.setTimestamp(item.getKey().toString(DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")));
			cur.setStatus(opsMgr.getStrStatus(item.getValue()));
			out.collect(cur);
		}

	}
}
