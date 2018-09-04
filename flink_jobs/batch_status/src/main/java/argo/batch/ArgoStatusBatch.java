package argo.batch;

import org.slf4j.LoggerFactory;

import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;
import argo.avro.MetricData;
import argo.avro.MetricProfile;
import ops.ConfigManager;

import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.operators.DataSource;

import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.core.fs.Path;


/**
 * Implements an ARGO Status Batch Job in flink
 * 
 * Submit job in flink cluster using the following parameters 
 * --mps: path tometric profile sync file (For hdfs use: hdfs://namenode:port/path/to/file)
 * --egp: path to endpoints group topology file (For hdfs use: hdfs://namenode:port/path/to/file) 
 * --ggp: path to group of groups topology file (For hdfs use: hdfs://namenode:port/path/to/file) 
 * --pdata: path to previous day's metric data file (For hdfs use: hdfs://namenode:port/path/to/file)
 * --mdata: path to metric data file (For hdfs use: hdfs://namenode:port/path/to/file)
 * --ops: path to operations profile file (For hdfs use: hdfs://namenode:port/path/to/file)
 * --aps: path to aggregations profile file (For hdfs use: hdfs://namenode:port/path/to/file)
 * --cfg: path to report's configuration file (For hdfs use: hdfs://namenode:port/path/to/file)
 * --rec: path to recomputations file
 * --run.date: target date of computation in DD-MM-YYYY format
 * --mongo.uri: path to MongoDB destination (eg mongodb://localhost:27017/database.table
 * --mongo.method: Method for storing results to Mongo (insert,upsert)
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
		// Fixed restart strategy: on failure attempt max 10 times to restart with a retry interval of 2 minutes
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(2, TimeUnit.MINUTES)));
		// sync data for input
		Path mps = new Path(params.getRequired("mps"));
		Path egp = new Path(params.getRequired("egp"));
		Path ggp = new Path(params.getRequired("ggp"));
		

		DataSource<String> cfgDS = env.readTextFile(params.getRequired("conf"));
		DataSource<String> opsDS = env.readTextFile(params.getRequired("ops"));
		DataSource<String> apsDS = env.readTextFile(params.getRequired("apr"));
		DataSource<String> recDS = env.readTextFile(params.getRequired("rec"));
		
		// begin with empty threshold datasource
		DataSource<String> thrDS = env.fromElements("");
		// if threshold filepath has been defined in cli parameters
		if (params.has("thr")){
			// read file and update threshold datasource
			thrDS = env.readTextFile(params.getRequired("thr"));
		}
		
		ConfigManager confMgr = new ConfigManager();
		confMgr.loadJsonString(cfgDS.collect());

		// Get conf data 
		List<String> confData = cfgDS.collect();
		ConfigManager cfgMgr = new ConfigManager();
		cfgMgr.loadJsonString(confData);
		// sync data input: metric profile in avro format
		AvroInputFormat<MetricProfile> mpsAvro = new AvroInputFormat<MetricProfile>(mps, MetricProfile.class);
		DataSet<MetricProfile> mpsDS = env.createInput(mpsAvro);

		// sync data input: endpoint group topology data in avro format
		AvroInputFormat<GroupEndpoint> egpAvro = new AvroInputFormat<GroupEndpoint>(egp, GroupEndpoint.class);
		DataSet<GroupEndpoint> egpDS = env.createInput(egpAvro);

		// sync data input: group of group topology data in avro format
		AvroInputFormat<GroupGroup> ggpAvro = new AvroInputFormat<GroupGroup>(ggp, GroupGroup.class);
		DataSet<GroupGroup> ggpDS = env.createInput(ggpAvro);

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
				.withBroadcastSet(opsDS, "ops");

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
		
		String reportID = cfgMgr.getReportID();
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