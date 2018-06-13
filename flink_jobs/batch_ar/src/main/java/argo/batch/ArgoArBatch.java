package argo.batch;

import org.slf4j.LoggerFactory;

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
 * <p>
 * The specific batch job calculates the availability and reliability results based on the input metric data
 * and sync files
 * </p>
 * Required arguments:
 * <ul>
 * <li>--pdata : file location of previous day's metric data (local or
 * hdfs)</li>
 * <li>--mdata : file location of target day's metric data (local or hdfs)</li>
 * <li>--egp : file location of endpoint group topology file (local or
 * hdfs)</li>
 * <li>--ggp : file location of group of groups topology file (local or
 * hdfs)</li>
 * <li>--mps : file location of metric profile (local or hdfs)</li>
 * <li>--aps : file location of aggregations profile (local or hdfs)</li>
 * <li>--ops : file location of operations profile (local or hdfs)</li>
 * <li>--rec : file location of recomputations file (local or hdfs)</li>
 * <li>--weights : file location of weights file (local or hdfs)</li>
 * <li>--downtimes : file location of downtimes file (local or hdfs)</li>
 * <li>--conf : file location of report configuration json file (local or
 * hdfs)</li>
 * <li>--run.date : target date in DD-MM-YYYY format</li>
 * <li>--mongo.uri : mongo uri for outputting the results</li>
 * <li>--mongo.method : mongo method for storing the results</li>
 * <ul>
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

		Path mps = new Path(params.getRequired("mps"));
		Path egp = new Path(params.getRequired("egp"));
		Path ggp = new Path(params.getRequired("ggp"));
		Path down = new Path(params.getRequired("downtimes"));
		Path weight = new Path(params.getRequired("weights"));

		DataSource<String> confDS = env.readTextFile(params.getRequired("conf"));
		DataSource<String> opsDS = env.readTextFile(params.getRequired("ops"));
		DataSource<String> aprDS = env.readTextFile(params.getRequired("apr"));
		DataSource<String> recDS = env.readTextFile(params.getRequired("rec"));
		
		ConfigManager confMgr = new ConfigManager();
		confMgr.loadJsonString(confDS.collect());

		// sync data input: metric profile in avro format
		AvroInputFormat<MetricProfile> mpsAvro = new AvroInputFormat<MetricProfile>(mps, MetricProfile.class);
		DataSet<MetricProfile> mpsDS = env.createInput(mpsAvro);

		// sync data input: endpoint group topology data in avro format
		AvroInputFormat<GroupEndpoint> egpAvro = new AvroInputFormat<GroupEndpoint>(egp, GroupEndpoint.class);
		DataSet<GroupEndpoint> egpDS = env.createInput(egpAvro);

		// sync data input: group of group topology data in avro format
		AvroInputFormat<GroupGroup> ggpAvro = new AvroInputFormat<GroupGroup>(ggp, GroupGroup.class);
		DataSet<GroupGroup> ggpDS = env.createInput(ggpAvro);

		// sync data input: downtime data in avro format
		AvroInputFormat<Downtime> downAvro = new AvroInputFormat<Downtime>(down, Downtime.class);
		DataSet<Downtime> downDS = env.createInput(downAvro);

		// sync data input: weight data in avro format
		AvroInputFormat<Weight> weightAvro = new AvroInputFormat<Weight>(weight, Weight.class);
		DataSet<Weight> weightDS = env.createInput(weightAvro);

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
				.withBroadcastSet(aprDS, "apr").withBroadcastSet(recDS, "rec").withBroadcastSet(confDS, "conf");

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

	    // Initialize two mongodb outputs
		MongoServiceArOutput serviceMongoOut = new MongoServiceArOutput(dbURI,"service_ar",dbMethod);
		 // Initialize two mongodb outputs
		MongoEndGroupArOutput egroupMongoOut = new MongoEndGroupArOutput(dbURI,"endpoint_group_ar",dbMethod);
		
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