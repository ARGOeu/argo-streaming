package argo.batch;

import org.slf4j.LoggerFactory;

import argo.amr.ApiResource;
import argo.amr.ApiResourceManager;
import argo.avro.Downtime;
import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;
import argo.avro.MetricData;
import argo.avro.MetricProfile;
import argo.avro.Weight;
import ops.ConfigManager;

import org.slf4j.Logger;
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
public class ArgoArBatch {
	// setup logger
	static Logger LOG = LoggerFactory.getLogger(ArgoArBatch.class);

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);
		env.setParallelism(1);
		// sync data for input
		
		
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
		
		

//		Path mps = new Path(params.getRequired("mps"));
//		Path egp = new Path(params.getRequired("egp"));
//		Path ggp = new Path(params.getRequired("ggp"));
//		Path down = new Path(params.getRequired("downtimes"));
//		Path weight = new Path(params.getRequired("weights"));
		

		//DataSource<String> confDS = env.readTextFile(params.getRequired("conf"));
//		DataSource<String> opsDS = env.readTextFile(params.getRequired("ops"));
//		DataSource<String> aprDS = env.readTextFile(params.getRequired("apr"));
//		DataSource<String> recDS = env.readTextFile(params.getRequired("rec"));
		
	
		DataSource<String>confDS = env.fromElements(amr.getResourceJSON(ApiResource.CONFIG));
		DataSource<String>opsDS = env.fromElements(amr.getResourceJSON(ApiResource.OPS));
		DataSource<String>aprDS = env.fromElements(amr.getResourceJSON(ApiResource.AGGREGATION));
		DataSource<String>recDS = env.fromElements(amr.getResourceJSON(ApiResource.RECOMPUTATIONS));
		
		
		// begin with empty threshold datasource
		DataSource<String> thrDS = env.fromElements("");
		
		// check if report information from argo-web-api contains a threshold profile ID
		if (!amr.getThresholdsID().equalsIgnoreCase("")){
			// grab information about thresholds rules from argo-web-api
			thrDS = env.fromElements(amr.getResourceJSON(ApiResource.THRESHOLDS));
		}
		
		
		DataSet<Downtime> downDS = env.fromElements(new Downtime());
		DataSet<Weight> weightDS = env.fromElements(new Weight());
		DataSet<GroupGroup> ggpDS = env.fromElements(new GroupGroup());
		
		ConfigManager confMgr = new ConfigManager();
		confMgr.loadJsonString(confDS.collect());

		// Get the sync datasets directly from the web-api data
		DataSet<MetricProfile> mpsDS = env.fromElements(amr.getListMetrics());
		DataSet<GroupEndpoint> egpDS = env.fromElements(amr.getListGroupEndpoints());
		
		
		Downtime[] listDowntimes = amr.getListDowntimes();
		Weight[] listWeights = amr.getListWeights();
		GroupGroup[] listGroups = amr.getListGroupGroups();
		
		if (listDowntimes.length > 0) downDS = env.fromElements(amr.getListDowntimes());
		if (listWeights.length > 0) weightDS = env.fromElements(amr.getListWeights());
		if (listGroups.length > 0) ggpDS = env.fromElements(amr.getListGroupGroups());
		


		// todays metric data
		Path in = new Path(params.getRequired("mdata"));
		AvroInputFormat<MetricData> mdataAvro = new AvroInputFormat<MetricData>(in, MetricData.class);
		DataSet<MetricData> mdataDS = env.createInput(mdataAvro);

		// previous metric data
		Path pin = new Path(params.getRequired("pdata"));
		AvroInputFormat<MetricData> pdataAvro = new AvroInputFormat<MetricData>(pin, MetricData.class);
		DataSet<MetricData> pdataDS = env.createInput(pdataAvro);

		// Find the latest day
		DataSet<MetricData> pdataMin = pdataDS.groupBy("service", "hostname", "metric")
				.sortGroup("timestamp", Order.DESCENDING).first(1);

		DataSet<MetricData> mdataPrevTotalDS = mdataDS.union(pdataMin);

		// Generate Full Missing dataset for the given topology
		DataSet<MonData> fillMissDS = mdataPrevTotalDS.reduceGroup(new FillMissing(params))
				.withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
				.withBroadcastSet(opsDS, "ops").withBroadcastSet(aprDS, "aps").withBroadcastSet(confDS, "conf");

		// Discard unused data and attach endpoint group as information
		DataSet<MonData> mdataTrimDS = mdataPrevTotalDS.flatMap(new PickEndpoints(params))
				.withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
				.withBroadcastSet(aprDS, "apr").withBroadcastSet(recDS, "rec").withBroadcastSet(confDS, "conf")
				.withBroadcastSet(opsDS, "ops").withBroadcastSet(thrDS, "thr");

		// Combine prev and todays metric data with the generated missing metric
		// data
		DataSet<MonData> mdataTotalDS = mdataTrimDS.union(fillMissDS);
		
		// Create a dataset of metric timelines
		DataSet<MonTimeline> metricTimelinesDS = mdataTotalDS.groupBy("group","service", "hostname", "metric")
				.sortGroup("timestamp", Order.ASCENDING).reduceGroup(new CreateMetricTimeline(params))
				.withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
				.withBroadcastSet(opsDS, "ops").withBroadcastSet(aprDS, "aps").withBroadcastSet(confDS, "conf");

		// Create a dataset of endpoint timelines
		DataSet<MonTimeline> endpointTimelinesDS = metricTimelinesDS.groupBy("group", "service", "hostname")
				.sortGroup("metric", Order.ASCENDING).reduceGroup(new CreateEndpointTimeline(params))
				.withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
				.withBroadcastSet(opsDS, "ops").withBroadcastSet(aprDS, "aps").withBroadcastSet(downDS, "down");

		// Create a dataset of service timelines
		DataSet<MonTimeline> serviceTimelinesDS = endpointTimelinesDS.groupBy("group", "service")
				.sortGroup("hostname", Order.ASCENDING).reduceGroup(new CreateServiceTimeline(params))
				.withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
				.withBroadcastSet(opsDS, "ops").withBroadcastSet(aprDS, "aps");

		// Create a dataset of endpoint group timelines
		DataSet<MonTimeline> groupTimelinesDS = serviceTimelinesDS.groupBy("group")
				.sortGroup("service", Order.ASCENDING).reduceGroup(new CreateEndpointGroupTimeline(params))
				.withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
				.withBroadcastSet(opsDS, "ops").withBroadcastSet(aprDS, "aps").withBroadcastSet(recDS, "rec");

		// Calculate endpoint ar from endpoint timelines
		DataSet<EndpointAR> endpointResultDS = endpointTimelinesDS.flatMap(new CalcEndpointAR(params))
				.withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
				.withBroadcastSet(aprDS, "apr").withBroadcastSet(recDS, "rec").withBroadcastSet(opsDS, "ops")
						.withBroadcastSet(confDS, "conf");
		
		// Calculate service ar from service timelines
		DataSet<ServiceAR> serviceResultDS = serviceTimelinesDS.flatMap(new CalcServiceAR(params))
				.withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
				.withBroadcastSet(aprDS, "apr").withBroadcastSet(recDS, "rec").withBroadcastSet(opsDS, "ops")
				.withBroadcastSet(confDS, "conf");

		// Calculate endpoint group ar from endpoint group timelines
		DataSet<EndpointGroupAR> groupResultDS = groupTimelinesDS.flatMap(new CalcEndpointGroupAR(params))
				.withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
				.withBroadcastSet(aprDS, "apr").withBroadcastSet(recDS, "rec").withBroadcastSet(opsDS, "ops")
				.withBroadcastSet(weightDS, "weight").withBroadcastSet(confDS, "conf");

		
		String dbURI = params.getRequired("mongo.uri");
		String dbMethod = params.getRequired("mongo.method");

		// Initialize endpoint ar mongo output 
		MongoEndpointArOutput endpointMongoOut = new MongoEndpointArOutput(dbURI,"endpoint_ar",dbMethod);
	    // Initialize service ar mongo output
		MongoServiceArOutput serviceMongoOut = new MongoServiceArOutput(dbURI,"service_ar",dbMethod);
		 // Initialize endpoint group ar mongo output
		MongoEndGroupArOutput egroupMongoOut = new MongoEndGroupArOutput(dbURI,"endpoint_group_ar",dbMethod);
		
		
		endpointResultDS.output(endpointMongoOut);
		serviceResultDS.output(serviceMongoOut);
		groupResultDS.output(egroupMongoOut);
		
		
		String runDate = params.getRequired("run.date");
		
		// Create a job title message to discern job in flink dashboard/cli
		StringBuilder jobTitleSB = new StringBuilder();
		jobTitleSB.append("Ar Batch job for tenant:");
		jobTitleSB.append(confMgr.getTenant());
		jobTitleSB.append("on day:");
		jobTitleSB.append(runDate);
		jobTitleSB.append("using report:");
		jobTitleSB.append(confMgr.getReport());
			
		env.execute(jobTitleSB.toString());
	

	}

	

	

}