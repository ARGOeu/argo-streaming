package argo.batch;

import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoOutputFormat;

import argo.avro.Downtime;
import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;
import argo.avro.MetricData;
import argo.avro.MetricProfile;
import argo.avro.Weight;

import org.slf4j.Logger;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.flink.api.java.io.AvroInputFormat;

import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;

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
		Path down = new Path(params.getRequired("downtimes"));
		Path weight = new Path(params.getRequired("weights"));

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

		// Calculate service ar from service timelines
		DataSet<ServiceAR> serviceResultDS = serviceTimelinesDS.flatMap(new CalcServiceAR(params))
				.withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
				.withBroadcastSet(aprDS, "apr").withBroadcastSet(recDS, "rec").withBroadcastSet(opsDS, "ops");

		// Calculate endpoint group ar from endpoint group timelines
		DataSet<EndpointGroupAR> groupResultDS = groupTimelinesDS.flatMap(new CalcEndpointGroupAR(params))
				.withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
				.withBroadcastSet(aprDS, "apr").withBroadcastSet(recDS, "rec").withBroadcastSet(opsDS, "ops")
				.withBroadcastSet(weightDS, "weight");

		// Convert service ar results into mongo friendly format
		DataSet<Tuple2<NullWritable, BSONWritable>> serviceArBSON = serviceResultDS.map(new ConvertServiceArToBson());

		// Convert endpoint group ar results into mongo friendly format
		DataSet<Tuple2<NullWritable, BSONWritable>> endpointGroupArBSON = groupResultDS.map(new ConvertEndpointGroupArToBson());

		// Initialize a new hadoop conf object to add mongo connector related
		// property
		JobConf serviceArConf = new JobConf();
		// Add mongo destination as given in parameters
		serviceArConf.set("mongo.output.uri", params.getRequired("datastore.uri") + ".service_ar");
		// Initialize MongoOutputFormat
		MongoOutputFormat<NullWritable, BSONWritable> serviceArMongoOF = new MongoOutputFormat<NullWritable, BSONWritable>();
		// Use HadoopOutputFormat as a wrapper around MongoOutputFormat to write
		// service ar results in mongo db
		serviceArBSON.output(new HadoopOutputFormat<NullWritable, BSONWritable>(serviceArMongoOF, serviceArConf));

		// Initialize a new hadoop conf object to add mongo connector related
		// property
		JobConf endpointGroupArConf = new JobConf();
		// Add mongo destination as given in parameters
		endpointGroupArConf.set("mongo.output.uri", params.getRequired("datastore.uri") + ".endpoint_group_ar");
		// Initialize MongoOutputFormat
		MongoOutputFormat<NullWritable, BSONWritable> endpointGroupArMongoOF = new MongoOutputFormat<NullWritable, BSONWritable>();
		// Use HadoopOutputFormat as a wrapper around MongoOutputFormat to write
		// service ar results in mongo db
		endpointGroupArBSON.output(new HadoopOutputFormat<NullWritable, BSONWritable>(endpointGroupArMongoOF, endpointGroupArConf));

		env.execute("Flink Ar Batch Job");

	}

	/**
	 * RichMap operator that converts a ServiceAR result object to BSONWritable
	 * for proper storage in mongodb
	 */
	static class ConvertServiceArToBson extends RichMapFunction<ServiceAR, Tuple2<NullWritable, BSONWritable>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void open(Configuration parameters) {

		}

		/**
		 * Prepares the data in BSONWritable values for mongo storage Each tuple
		 * is in the form <K,V> and the key here must be empty for mongo to
		 * assign an ObjectKey NullWriteable as the first object of the tuple
		 * ensures an empty key
		 */
		@Override
		public Tuple2<NullWritable, BSONWritable> map(ServiceAR item) throws Exception {

			// Create a mongo database object with the needed fields
			DBObject builder = BasicDBObjectBuilder.start().add("report", item.getReport())
					.add("date", item.getDateInt()).add("name", item.getName()).add("supergroup", item.getGroup())
					.add("availability", item.getA()).add("reliability", item.getR()).add("up", item.getUp())
					.add("unknown", item.getUnknown()).add("down", item.getDown()).get();

			// Convert database object to BsonWriteable
			BSONWritable w = new BSONWritable(builder);

			return new Tuple2<NullWritable, BSONWritable>(NullWritable.get(), w);
		}
	}

	/**
	 * Rich map operator that converts an EndpointGroupAR result object to
	 * BSONWritable for proper storage in mongodb
	 */
	static class ConvertEndpointGroupArToBson
			extends RichMapFunction<EndpointGroupAR, Tuple2<NullWritable, BSONWritable>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void open(Configuration parameters) {

		}

		@Override
		/**
		 * Prepares the data in BSONWritable values for mongo storage Each tuple
		 * is in the form <K,V> and the key here must be empty for mongo to
		 * assign an ObjectKey NullWriteable as the first object of the tuple
		 * ensures an empty key
		 */
		public Tuple2<NullWritable, BSONWritable> map(EndpointGroupAR item) throws Exception {

			// Create a mongo database object with the needed fields
			DBObject builder = BasicDBObjectBuilder.start().add("report", item.getReport())
					.add("date", item.getDateInt()).add("name", item.getName()).add("supergroup", item.getGroup())
					.add("weight", item.getWeight()).add("availability", item.getA()).add("reliability", item.getR())
					.add("up", item.getUp()).add("unknown", item.getUnknown()).add("down", item.getDown()).get();

			// Convert database object to BsonWriteable
			BSONWritable w = new BSONWritable(builder);

			return new Tuple2<NullWritable, BSONWritable>(NullWritable.get(), w);
		}
	}

}