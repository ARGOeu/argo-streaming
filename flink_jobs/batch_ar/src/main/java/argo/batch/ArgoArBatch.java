package argo.batch;

import org.slf4j.LoggerFactory;

import argo.avro.Downtime;
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
 * Represents an ARGO Batch Job in flink
 * 
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
		Path down = new Path(params.getRequired("down"));

		DataSource<String> opsDS = env.readTextFile(params.getRequired("ops"));
		DataSource<String> aprDS = env.readTextFile(params.getRequired("apr"));
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

		// sync data input: downtime data in avro format
		AvroInputFormat<Downtime> downAvro = new AvroInputFormat<Downtime>(down, Downtime.class);
		DataSet<Downtime> downDS = env.createInput(downAvro);

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
		DataSet<MetricData> mdataTrimDS = mdataTotalDS.flatMap(new PickEndpoints(params)).withBroadcastSet(mpsDS, "mps")
				.withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp").withBroadcastSet(aprDS, "apr")
				.withBroadcastSet(recDS, "rec");

		// Create a dataset of metric timelines
		DataSet<MonTimeline> metricTimelinesDS = mdataTrimDS.groupBy("service", "hostname", "metric")
				.sortGroup("timestamp", Order.ASCENDING).reduceGroup(new CreateMetricTimeline(params))
				.withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
				.withBroadcastSet(opsDS, "ops").withBroadcastSet(aprDS, "aps");

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

		// Discard unused data and attach endpoint group as information
		DataSet<ServiceAR> serviceResultDS = serviceTimelinesDS.flatMap(new CalcServiceAR(params))
				.withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
				.withBroadcastSet(aprDS, "apr").withBroadcastSet(recDS, "rec").withBroadcastSet(opsDS, "ops");

		serviceResultDS.writeAsText("/tmp/batch-ar-output");

		env.execute("Flink Ar Batch Job");

	}

}