package argo.batch;

import org.slf4j.LoggerFactory;

import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;
import argo.avro.MetricData;
import argo.avro.MetricProfile;

import org.slf4j.Logger;

import org.apache.flink.api.common.operators.Order;
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
 * --mdata: path to previous day's metric data file (For hdfs use: hdfs://namenode:port/path/to/file)
 * --mdata: path to metric data file (For hdfs use: hdfs://namenode:port/path/to/file)
 * --ops: path to operations profile file (For hdfs use: hdfs://namenode:port/path/to/file)
 * --aps: path to aggregations profile file (For hdfs use: hdfs://namenode:port/path/to/file)
 * --rec: path to recomputations file
 * --run.date: target date of computation in DD-MM-YYYY format
 * --report: report uuid
 * --egroup.type: specify the type of the engpoint groups used in the report (e.g. SITES)
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
		// sync data for input
		Path mps = new Path(params.getRequired("mps"));
		Path egp = new Path(params.getRequired("egp"));
		Path ggp = new Path(params.getRequired("ggp"));

		DataSource<String> opsDS = env.readTextFile(params.getRequired("ops"));
		DataSource<String> apsDS = env.readTextFile(params.getRequired("aps"));
		DataSource<String> recDS = env.readTextFile(params.getRequired("rec"));

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

		DataSet<MetricData> mdataTotalDS = mdataDS.union(pdataMin);

		// Discard unused data and attach endpoint group as information
		DataSet<StatusMetric> mdataTrimDS = mdataTotalDS.flatMap(new PickEndpoints(params))
				.withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
				.withBroadcastSet(recDS, "rec");

		// Create status detail data set
		DataSet<StatusMetric> stDetailDS = mdataTrimDS.groupBy("group", "service", "hostname", "metric")
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
		String report = params.getRequired("report");
		
		 // Initialize four mongo outputs (metric,endpoint,service,endpoint_group)
		MongoStatusOutput metricMongoOut = new MongoStatusOutput(dbURI,"status_metrics",dbMethod, MongoStatusOutput.StatusType.STATUS_METRIC, report);
		MongoStatusOutput endpointMongoOut = new MongoStatusOutput(dbURI,"status_endpoints",dbMethod, MongoStatusOutput.StatusType.STATUS_ENDPOINT, report);
		MongoStatusOutput serviceMongoOut = new MongoStatusOutput(dbURI,"status_services",dbMethod, MongoStatusOutput.StatusType.STATUS_ENDPOINT, report);
		MongoStatusOutput endGroupMongoOut = new MongoStatusOutput(dbURI,"status_endpoint_groups",dbMethod, MongoStatusOutput.StatusType.STATUS_ENDPOINT_GROUP, report);
		
		// Store datasets to the designated outputs prepared above
		stDetailDS.output(metricMongoOut);
		stEndpointDS.output(endpointMongoOut);
		stServiceDS.output(serviceMongoOut);
		stEndGroupDS.output(endGroupMongoOut);

		env.execute("Flink Status Batch Job");

	}

}