package argo.batch;

import org.slf4j.LoggerFactory;

import argo.amr.ApiResource;
import argo.amr.ApiResourceManager;
import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;
import argo.avro.MetricData;
import argo.avro.MetricProfile;
import ops.ConfigManager;

import org.slf4j.Logger;

import java.util.List;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.operators.DataSource;

import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.core.fs.Path;


/**
 * Represents an ARGO A/R Batch Job in flink
 * 
 * Submit job in flink cluster using the following parameters: 
 * --pdata: path to previous day's metric data file (For hdfs use: hdfs://namenode:port/path/to/file)
 * --mdata: path to metric data file (For hdfs use: hdfs://namenode:port/path/to/file)
 * --run.date: target date of computation in DD-MM-YYYY format
 * --mongo.uri: path to MongoDB destination (eg mongodb://localhost:27017/database.table
 * --mongo.method: Method for storing results to Mongo (insert,upsert)
 * --report.id: UUUID of the report
 * --api.endpoint: endpoint hostname of the argo-web-api instance (api.argo.example.com)
 * --api.token: access token to argo-web-api 
 * --api.proxy: optional address for proxy to be used (http://proxy.example.com)
 * --api.timeout: set timeout (in seconds) when connecting to argo-web-api
 */



public class ArgoStatusBatch {
	// setup logger
	static Logger LOG = LoggerFactory.getLogger(ArgoStatusBatch.class);

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);
		env.setParallelism(1);
		
		String apiEndpoint = params.getRequired("api.endpoint");
		String apiToken = params.getRequired("api.token");
		String reportID = params.getRequired("report.id");

		ApiResourceManager amr = new ApiResourceManager(apiEndpoint,apiToken);

		// fetch

		// set params
		if (params.has("api.proxy")) {
			amr.setProxy(params.get("api.proxy"));
		}
		
		if (params.has("api.timeout")) {
			amr.setTimeoutSec(params.getInt("api.timeout"));
		}

		amr.setReportID(reportID);
		amr.setDate(params.getRequired("run.date"));
		amr.getRemoteAll();
		

		DataSource<String>cfgDS = env.fromElements(amr.getResourceJSON(ApiResource.CONFIG));
		DataSource<String>opsDS = env.fromElements(amr.getResourceJSON(ApiResource.OPS));
		DataSource<String>apsDS = env.fromElements(amr.getResourceJSON(ApiResource.AGGREGATION));
		DataSource<String>recDS = env.fromElements(amr.getResourceJSON(ApiResource.RECOMPUTATIONS));
		
		// begin with empty threshold datasource
		DataSource<String> thrDS = env.fromElements("");
		
		// check if report information from argo-web-api contains a threshold profile ID
		if (!amr.getThresholdsID().equalsIgnoreCase("")){
			// grab information about thresholds rules from argo-web-api
			thrDS = env.fromElements(amr.getResourceJSON(ApiResource.THRESHOLDS));
		}
		
		ConfigManager confMgr = new ConfigManager();
		confMgr.loadJsonString(cfgDS.collect());

		// Get conf data 
		List<String> confData = cfgDS.collect();
		ConfigManager cfgMgr = new ConfigManager();
		cfgMgr.loadJsonString(confData);
		
		DataSet<MetricProfile> mpsDS = env.fromElements(amr.getListMetrics());
		DataSet<GroupEndpoint> egpDS = env.fromElements(amr.getListGroupEndpoints());
		DataSet<GroupGroup> ggpDS = env.fromElements(new GroupGroup());
		GroupGroup[] listGroups = amr.getListGroupGroups();
		if (listGroups.length > 0) ggpDS = env.fromElements(amr.getListGroupGroups());
		
		
		
		// todays metric data
		Path in = new Path(params.getRequired("mdata"));
		AvroInputFormat<MetricData> mdataAvro = new AvroInputFormat<MetricData>(in, MetricData.class);
		DataSet<MetricData> mdataDS = env.createInput(mdataAvro);

		// previous metric data
		Path pin = new Path(params.getRequired("pdata"));
		AvroInputFormat<MetricData> pdataAvro = new AvroInputFormat<MetricData>(pin, MetricData.class);
		DataSet<MetricData> pdataDS = env.createInput(pdataAvro);
		
		DataSet<MetricData> pdataCleanDS = pdataDS.flatMap(new ExcludeMetricData(params)).withBroadcastSet(recDS, "rec");

		// Find the latest day
		DataSet<MetricData> pdataMin = pdataCleanDS.groupBy("service", "hostname", "metric")
				.sortGroup("timestamp", Order.DESCENDING).first(1);
		
		// Union todays data with the latest statuses from previous day 
		DataSet<MetricData> mdataPrevTotalDS = mdataDS.union(pdataMin);
		
		// Use yesterday's latest statuses and todays data to find the missing ones and add them to the mix
		DataSet<StatusMetric> fillMissDS = mdataPrevTotalDS.reduceGroup(new FillMissing(params))
				.withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
				.withBroadcastSet(opsDS, "ops").withBroadcastSet(cfgDS, "conf");
		

		// Discard unused data and attach endpoint group as information
		DataSet<StatusMetric> mdataTrimDS = mdataPrevTotalDS.flatMap(new PickEndpoints(params))
				.withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
				.withBroadcastSet(recDS, "rec").withBroadcastSet(cfgDS, "conf").withBroadcastSet(thrDS, "thr")
				.withBroadcastSet(opsDS, "ops").withBroadcastSet(apsDS, "aps");

		// Combine prev and todays metric data with the generated missing metric
		// data
		DataSet<StatusMetric> mdataTotalDS = mdataTrimDS.union(fillMissDS);
		
		// Create status detail data set
		DataSet<StatusMetric> stDetailDS = mdataTotalDS.groupBy("group", "service", "hostname", "metric")
				.sortGroup("timestamp", Order.ASCENDING).reduceGroup(new CalcPrevStatus(params))
				.withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp");
						
		
		// Create status endpoint data set
		DataSet<StatusMetric> stEndpointDS = stDetailDS.groupBy("group", "service", "hostname")
				.sortGroup("metric", Order.ASCENDING).sortGroup("timestamp", Order.ASCENDING)
				.reduceGroup(new CalcStatusEndpoint(params)).withBroadcastSet(mpsDS, "mps")
				.withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp").withBroadcastSet(opsDS, "ops")
				.withBroadcastSet(apsDS, "aps");

		
		// Create status service data set
		DataSet<StatusMetric> stServiceDS = stEndpointDS.groupBy("group", "service")
				.sortGroup("hostname", Order.ASCENDING).sortGroup("timestamp", Order.ASCENDING)
				.reduceGroup(new CalcStatusService(params)).withBroadcastSet(mpsDS, "mps")
				.withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp").withBroadcastSet(opsDS, "ops")
				.withBroadcastSet(apsDS, "aps");
		

		// Create status endpoint group data set
		DataSet<StatusMetric> stEndGroupDS = stServiceDS.groupBy("group").sortGroup("service", Order.ASCENDING)
				.sortGroup("timestamp", Order.ASCENDING).reduceGroup(new CalcStatusEndGroup(params))
				.withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
				.withBroadcastSet(opsDS, "ops").withBroadcastSet(apsDS, "aps");


		String dbURI = params.getRequired("mongo.uri");
		String dbMethod = params.getRequired("mongo.method");
		
	
		 // Initialize four mongo outputs (metric,endpoint,service,endpoint_group)
		MongoStatusOutput metricMongoOut = new MongoStatusOutput(dbURI,"status_metrics",dbMethod, MongoStatusOutput.StatusType.STATUS_METRIC, reportID);
		MongoStatusOutput endpointMongoOut = new MongoStatusOutput(dbURI,"status_endpoints",dbMethod, MongoStatusOutput.StatusType.STATUS_ENDPOINT, reportID);
		MongoStatusOutput serviceMongoOut = new MongoStatusOutput(dbURI,"status_services",dbMethod, MongoStatusOutput.StatusType.STATUS_ENDPOINT, reportID);
		MongoStatusOutput endGroupMongoOut = new MongoStatusOutput(dbURI,"status_endpoint_groups",dbMethod, MongoStatusOutput.StatusType.STATUS_ENDPOINT_GROUP, reportID);
		
		// Store datasets to the designated outputs prepared above
		stDetailDS.output(metricMongoOut);
		stEndpointDS.output(endpointMongoOut);
		stServiceDS.output(serviceMongoOut);
		stEndGroupDS.output(endGroupMongoOut);

		String runDate = params.getRequired("run.date");
		
		// Create a job title message to discern job in flink dashboard/cli
		StringBuilder jobTitleSB = new StringBuilder();
		jobTitleSB.append("Status Batch job for tenant:");
		jobTitleSB.append(confMgr.getTenant());
		jobTitleSB.append(" on day:");
		jobTitleSB.append(runDate);
		jobTitleSB.append(" using report:");
		jobTitleSB.append(confMgr.getReport());					
		
		env.execute(jobTitleSB.toString());

	}

}