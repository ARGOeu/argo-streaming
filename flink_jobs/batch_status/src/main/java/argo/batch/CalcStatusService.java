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
import org.mortbay.log.Log;
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
 * Accepts a list o status metrics grouped by the fields: endpoint group, service
 * Uses Continuous Timelines and Aggregators to calculate the status results of a service flavor
 * Prepares the data in a form aligned with the datastore schema for status flavor collection
 */
public class CalcStatusService extends RichGroupReduceFunction<StatusMetric, StatusMetric> {

	private static final long serialVersionUID = 1L;

	final ParameterTool params;

	public CalcStatusService(ParameterTool params) {
		this.params = params;
	}

	static Logger LOG = LoggerFactory.getLogger(ArgoStatusBatch.class);

	
	
	private List<String> aps;
	private List<String> ops;
	
	private AggregationProfileManager apsMgr;
	private OpsManager opsMgr;
	

	private String runDate;
	private CAggregator serviceAggr;

	private boolean getService;
	
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
		this.serviceAggr = new CAggregator(); // Create aggregator

		this.getService = true;
	}

	@Override
	public void reduce(Iterable<StatusMetric> in, Collector<StatusMetric> out) throws Exception {

		this.serviceAggr.clear();

		String aProfile = this.apsMgr.getAvProfiles().get(0);
		String avGroup = "";
		String service ="";
		String endpointGroup ="";
		int dateInt = Integer.parseInt(this.runDate.replace("-", ""));

		
		for (StatusMetric item : in) {
						
			if (getService){
				
				service = item.getService();

				// Get the availability Group in which this service belongs
				avGroup = this.apsMgr.getGroupByService(aProfile, service);			
				
				getService =false;
					
			}
	
			
			service = item.getService();
			endpointGroup = item.getGroup();
			String hostname = item.getHostname();
			String ts = item.getTimestamp();
			String status = item.getStatus();
			
		
			this.serviceAggr.insert(hostname, ts, this.opsMgr.getIntStatus(status));
			
		}

		
		avGroup = this.apsMgr.getGroupByService(aProfile, service);
		String avOp = this.apsMgr.getProfileGroupServiceOp(aProfile, avGroup, service);
		
		this.serviceAggr.aggregate(this.opsMgr, "OR");

		// Append the timeline	
		for (Entry<DateTime, Integer> item : this.serviceAggr.getSamples()) {
			
			StatusMetric cur = new StatusMetric();
			cur.setDateInt(dateInt);
			cur.setGroup(endpointGroup);
			cur.setService(service);
			
			
			cur.setTimestamp(item.getKey().toString(DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")));
			cur.setStatus(opsMgr.getStrStatus(item.getValue()));
			out.collect(cur);
		}

	}

}
