package argo.batch;

import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

import com.mongodb.hadoop.mapred.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;


import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;
import argo.avro.MetricData;
import argo.avro.MetricProfile;
import sync.EndpointGroupManager;
import sync.GroupGroupManager;
import sync.MetricProfileManager;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.analysis.function.Add;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.io.TextOutputFormat;
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
 * --datastore.uri: path to datastore destination (eg mongodb://localhost:27017/database.table
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
				.withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp");

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


		
		/**
		 * Prepares the data in BSONWritable values for mongo storage Each tuple
		 * is in the form <K,V> and the key here must be empty for mongo to
		 * assign an ObjectKey NullWriteable as the first object of the tuple
		 * ensures an empty key
		 */
		DataSet<Tuple2<NullWritable, BSONWritable>> statusMetricBSON = stDetailDS
				.map(new RichMapFunction<StatusMetric, Tuple2<NullWritable, BSONWritable>>() {

					private static final long serialVersionUID = 1L;

					private String report;

					@Override
					public void open(Configuration parameters) {

						report = params.getRequired("report");

					}

					@Override
					public Tuple2<NullWritable, BSONWritable> map(StatusMetric md) throws Exception {

						// Create a mongo database object with the needed fields
						DBObject builder = BasicDBObjectBuilder.start().add("report", this.report)
								.add("service", md.getService()).add("hostname", md.getHostname())
								.add("metric", md.getMetric()).add("status", md.getStatus())
								.add("timestamp", md.getTimestamp()).add("date_integer", md.getDateInt())
								.add("time_integer", md.getTimeInt()).add("prev_status", md.getPrevState())
								.add("prev_ts", md.getPrevTs()).get();

						// Convert database object to BsonWriteable
						BSONWritable w = new BSONWritable(builder);

						return new Tuple2<NullWritable, BSONWritable>(NullWritable.get(), w);
					}
				});

		/**
		 * Prepares the data in BSONWritable values for mongo storage Each tuple
		 * is in the form <K,V> and the key here must be empty for mongo to
		 * assign an ObjectKey NullWriteable as the first object of the tuple
		 * ensures an empty key
		 */
		DataSet<Tuple2<NullWritable, BSONWritable>> statusEndpointBSON = stEndpointDS
				.map(new RichMapFunction<StatusMetric, Tuple2<NullWritable, BSONWritable>>() {

					private static final long serialVersionUID = 1L;

					private String report;

					@Override
					public void open(Configuration parameters) {

						report = params.getRequired("report");

					}

					@Override
					public Tuple2<NullWritable, BSONWritable> map(StatusMetric md) throws Exception {

						// Create a mongo database object with the needed fields
						DBObject builder = BasicDBObjectBuilder.start().add("report", this.report)
								.add("endpoint_group", md.getGroup()).add("service", md.getService())
								.add("hostname", md.getHostname()).add("status", md.getStatus())
								.add("timestamp", md.getTimestamp()).add("date_integer", md.getDateInt()).get();

						// Convert database object to BsonWriteable
						BSONWritable w = new BSONWritable(builder);

						return new Tuple2<NullWritable, BSONWritable>(NullWritable.get(), w);
					}
				});

		/**
		 * Prepares the data in BSONWritable values for mongo storage Each tuple
		 * is in the form <K,V> and the key here must be empty for mongo to
		 * assign an ObjectKey NullWriteable as the first object of the tuple
		 * ensures an empty key
		 */
		DataSet<Tuple2<NullWritable, BSONWritable>> statusServiceBSON = stServiceDS
				.map(new RichMapFunction<StatusMetric, Tuple2<NullWritable, BSONWritable>>() {

					private static final long serialVersionUID = 1L;

					private String report;

					@Override
					public void open(Configuration parameters) {

						report = params.getRequired("report");

					}

					@Override
					public Tuple2<NullWritable, BSONWritable> map(StatusMetric md) throws Exception {

						// Create a mongo database object with the needed fields
						DBObject builder = BasicDBObjectBuilder.start().add("report", this.report)
								.add("endpoint_group", md.getGroup()).add("service", md.getService())
								.add("status", md.getStatus()).add("timestamp", md.getTimestamp())
								.add("date_integer", md.getDateInt()).get();

						// Convert database object to BsonWriteable
						BSONWritable w = new BSONWritable(builder);

						return new Tuple2<NullWritable, BSONWritable>(NullWritable.get(), w);
					}
				});

		/**
		 * Prepares the data in BSONWritable values for mongo storage Each tuple
		 * is in the form <K,V> and the key here must be empty for mongo to
		 * assign an ObjectKey NullWriteable as the first object of the tuple
		 * ensures an empty key
		 */
		DataSet<Tuple2<NullWritable, BSONWritable>> statusEndGroupBSON = stEndGroupDS
				.map(new RichMapFunction<StatusMetric, Tuple2<NullWritable, BSONWritable>>() {

					private static final long serialVersionUID = 1L;

					private String report;

					@Override
					public void open(Configuration parameters) {

						report = params.getRequired("report");

					}

					@Override
					public Tuple2<NullWritable, BSONWritable> map(StatusMetric md) throws Exception {

						// Create a mongo database object with the needed fields
						DBObject builder = BasicDBObjectBuilder.start().add("report", this.report)
								.add("endpoint_group", md.getGroup()).add("service", md.getService())
								.add("status", md.getStatus()).add("timestamp", md.getTimestamp())
								.add("date_integer", md.getDateInt()).get();

						// Convert database object to BsonWriteable
						BSONWritable w = new BSONWritable(builder);

						return new Tuple2<NullWritable, BSONWritable>(NullWritable.get(), w);
					}
				});

		
		
		// Initialize a new hadoop conf object to add mongo connector related
		// property
		JobConf conf = new JobConf();
		// Add mongo destination as given in parameters
		conf.set("mongo.output.uri", params.getRequired("datastore.uri") + ".status_metrics");
		// Initialize MongoOutputFormat
		MongoOutputFormat<NullWritable, BSONWritable> mongoOutputFormat = new MongoOutputFormat<NullWritable, BSONWritable>();
		// Use HadoopOutputFormat as a wrapper around MongoOutputFormat to write
		// results in mongo db
		statusMetricBSON.output(new HadoopOutputFormat<NullWritable, BSONWritable>(mongoOutputFormat, conf));

		
		
		// Initialize a new hadoop conf object to add mongo connector related
		// property
		JobConf conf2 = new JobConf();
		// Add mongo destination as given in parameters
		conf2.set("mongo.output.uri", params.getRequired("datastore.uri") + ".status_endpoints");
		// Initialize MongoOutputFormat
		MongoOutputFormat<NullWritable, BSONWritable> mongoOutputFormat2 = new MongoOutputFormat<NullWritable, BSONWritable>();
		// Use HadoopOutputFormat as a wrapper around MongoOutputFormat to write
		// results in mongo db
		statusEndpointBSON.output(new HadoopOutputFormat<NullWritable, BSONWritable>(mongoOutputFormat2, conf2));

		
		// Initialize a new hadoop conf object to add mongo connector related
		// property
		JobConf conf3 = new JobConf();
		// Add mongo destination as given in parameters
		conf3.set("mongo.output.uri", params.getRequired("datastore.uri") + ".status_services");
		// Initialize MongoOutputFormat
		MongoOutputFormat<NullWritable, BSONWritable> mongoOutputFormat3 = new MongoOutputFormat<NullWritable, BSONWritable>();
		// Use HadoopOutputFormat as a wrapper around MongoOutputFormat to write
		// results in mongo db
		statusServiceBSON.output(new HadoopOutputFormat<NullWritable, BSONWritable>(mongoOutputFormat3, conf3));

		
		// Initialize a new hadoop conf object to add mongo connector related
		// property
		JobConf conf4 = new JobConf();
		// Add mongo destination as given in parameters
		conf4.set("mongo.output.uri", params.getRequired("datastore.uri") + ".status_endpoint_groups");
		// Initialize MongoOutputFormat
		MongoOutputFormat<NullWritable, BSONWritable> mongoOutputFormat4 = new MongoOutputFormat<NullWritable, BSONWritable>();
		// Use HadoopOutputFormat as a wrapper around MongoOutputFormat to write
		// results in mongo db
		statusEndGroupBSON.output(new HadoopOutputFormat<NullWritable, BSONWritable>(mongoOutputFormat4, conf4));

		env.execute("Flink Status Batch Job");

	}

}