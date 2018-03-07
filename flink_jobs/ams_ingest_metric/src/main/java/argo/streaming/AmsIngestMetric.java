package argo.streaming;


import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.codec.binary.Base64;
import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.util.Collector;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.google.gson.JsonElement;

import com.google.gson.JsonParser;

import argo.avro.MetricData;


/**
 * Flink Job : Stream metric data from ARGO messaging to Hbase
 * job required cli parameters:
 * 
 * --ams.endpoint      : ARGO messaging api endoint to connect to msg.example.com
 * --ams.port          : ARGO messaging api port 
 * --ams.token         : ARGO messaging api token
 * --ams.project       : ARGO messaging api project to connect to
 * --ams.sub           : ARGO messaging subscription to pull from
 * --hbase.master      : hbase endpoint
 * --hbase.master.port : hbase master port
 * --hbase.zk.quorum   : comma separated list of hbase zookeeper servers
 * --hbase.zk.port     : port used by hbase zookeeper servers
 * --hbase.namespace   : table namespace used (usually tenant name)
 * --hbase.table       : table name (usually metric_data)
 * --check.path        : checkpoint path
 * --check.interval    : checkpoint interval
 * --hdfs.path         : hdfs destination to write the data
 * --ams.batch         : num of messages to be retrieved per request to AMS service
 * --ams.interval      : interval (in ms) between AMS service requests
 */
public class AmsIngestMetric {
	// setup logger
	static Logger LOG = LoggerFactory.getLogger(AmsIngestMetric.class);

	/**
	 * Check if flink job has been called with ams rate params
	 */
	public static boolean hasAmsRateArgs(ParameterTool paramTool) {
		String args[] = { "ams.batch", "ams.interval" };
		return hasArgs(args, paramTool);
	}

	
	/**
	 * Check if flink job has been called with checkpoint cli arguments
	 */
	public static boolean hasCheckArgs(ParameterTool paramTool) {
		String args[] = { "check.path", "check.interval" };
		return hasArgs(args, paramTool);
	}

	/**
	 * Check if flink job has been called with hdfs cli arguments
	 */
	public static boolean hasHdfsArgs(ParameterTool paramTool) {
		String args[] = { "hdfs.path" };
		return hasArgs(args, paramTool);
	}

	/**
	 * Check if flink job has been called with hbase cli arguments
	 */
	public static boolean hasHbaseArgs(ParameterTool paramTool) {
		String args[] = { "hbase.master", "hbase.master.port", "hbase.zk.quorum", "hbase.zk.port", "hbase.namespace",
				"hbase.table" };
		return hasArgs(args, paramTool);
	}

	/**
	 * Check if a list of expected cli arguments have been provided to this flink job
	 */
	public static boolean hasArgs(String[] reqArgs, ParameterTool paramTool) {

		for (String reqArg : reqArgs) {
			if (!paramTool.has(reqArg))
				return false;
		}

		return true;
	}

	public static void main(String[] args) throws Exception {

		// Create flink execution environment
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setParallelism(1);

		// Initialize cli parameter tool
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);
		
		// set ams client batch and interval to default values
		int batch = 1;
		long interval = 100L;
		long inactivityThresh = 1800000L; // default inactivity threshold value ~ 30mins
		
		if (hasAmsRateArgs(parameterTool)) {
			batch = parameterTool.getInt("ams.batch");
			interval = parameterTool.getLong("ams.interval");
		}
		

		// Initialize Input Source : ARGO Messaging Source
		String endpoint = parameterTool.getRequired("ams.endpoint");
		String port = parameterTool.getRequired("ams.port");
		String token = parameterTool.getRequired("ams.token");
		String project = parameterTool.getRequired("ams.project");
		String sub = parameterTool.getRequired("ams.sub");
		
		
		// Check if checkpointing is desired
		if (hasCheckArgs(parameterTool)) {
			String checkPath = parameterTool.get("check.path");
			String checkInterval = parameterTool.get("check.interval");
			// Establish check-pointing mechanism using the cli-parameter check.path
			see.setStateBackend(new FsStateBackend(checkPath));
			// Establish the check-pointing interval
			long checkInt = Long.parseLong(checkInterval);
			see.enableCheckpointing(checkInt);
		}

		// Ingest sync avro encoded data from AMS endpoint
		DataStream<String> metricDataJSON = see.addSource(new ArgoMessagingSource(endpoint, port, token, project, sub, batch, interval));
		DataStream<MetricData> metricDataPOJO = metricDataJSON.flatMap(new FlatMapFunction<String, MetricData>() {

			/**
			 * Flat Map Function that accepts AMS message and exports the metric data object (encoded in the payload)
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void flatMap(String value, Collector<MetricData> out) throws Exception {

				JsonParser jsonParser = new JsonParser();
				// parse the json root object
				JsonElement jRoot = jsonParser.parse(value);
				// parse the json field "data" and read it as string
				// this is the base64 string payload
				String data = jRoot.getAsJsonObject().get("data").getAsString();
				// Decode from base64
				byte[] decoded64 = Base64.decodeBase64(data.getBytes("UTF-8"));
				// Decode from avro
				DatumReader<MetricData> avroReader = new SpecificDatumReader<MetricData>(MetricData.getClassSchema(),
						MetricData.getClassSchema(), new SpecificData());
				Decoder decoder = DecoderFactory.get().binaryDecoder(decoded64, null);
				MetricData item;
				item = avroReader.read(null, decoder);
				if (item != null) {
					out.collect(item);
				}

			}
		});
		
		// Check if saving to HDFS is desired
		if (hasHdfsArgs(parameterTool)) {
			String basePath = parameterTool.getRequired("hdfs.path");
			// Establish a bucketing sink to be able to store events with different daily
			// timestamp parts (YYYY-MM-DD)
			// in different daily files
			BucketingSink<MetricData> bs = new BucketingSink<MetricData>(basePath);
			bs.setInactiveBucketThreshold(inactivityThresh);
			Bucketer<MetricData> tsBuck = new TSBucketer();
			bs.setBucketer(tsBuck);
			bs.setPartPrefix("mdata");
			// Change default in progress prefix: _ to allow loading the file in
			// AvroInputFormat
			bs.setInProgressPrefix("");
			// Add .prog extension when a file is in progress mode
			bs.setInProgressSuffix(".prog");
			// Add .pend extension when a file is in pending mode
			bs.setPendingSuffix(".pend");

			bs.setWriter(new SpecificAvroWriter<MetricData>());
			metricDataPOJO.addSink(bs);
		}
		
		// Check if saving to Hbase is desired
		if (hasHbaseArgs(parameterTool)) {
			// Initialize Output : Hbase Output Format
			HBaseMetricOutputFormat hbf = new HBaseMetricOutputFormat();
			hbf.setMaster(parameterTool.getRequired("hbase.master"));
			hbf.setMasterPort(parameterTool.getRequired("hbase.master-port"));
			hbf.setZkQuorum(parameterTool.getRequired("hbase.zk.quorum"));
			hbf.setZkPort(parameterTool.getRequired("hbase.zk.port"));
			hbf.setNamespace(parameterTool.getRequired("hbase.namespace"));
			hbf.setTableName(parameterTool.getRequired("hbase.table"));
			
			metricDataPOJO.writeUsingOutputFormat(hbf);
		}
		
		// Create a job title message to discern job in flink dashboard/cli
		StringBuilder jobTitleSB = new StringBuilder();
		jobTitleSB.append("Ingesting metric data from ");
		jobTitleSB.append(endpoint);
		jobTitleSB.append(":");
		jobTitleSB.append(port);
		jobTitleSB.append("/v1/projects/");
		jobTitleSB.append(project);
		jobTitleSB.append("/subscriptions/");
		jobTitleSB.append(sub);
		
		see.execute(jobTitleSB.toString());
	}

}