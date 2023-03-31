package argo.streaming;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.codec.binary.Base64;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.compactor.DecoderBasedReader;
import org.apache.flink.connector.file.sink.compactor.FileCompactStrategy;
import org.apache.flink.connector.file.sink.compactor.RecordWiseFileCompactor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.Collector;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.google.gson.JsonElement;

import com.google.gson.JsonParser;

import argo.avro.MetricData;
import argo.avro.MetricDataOld;


/**
 * Flink Job : Stream metric data from ARGO messaging to Hbase
 * job required cli parameters:
 * 
 * --ams.endpoint      : ARGO messaging api endoint to connect to msg.example.com
 * --ams.port          : ARGO messaging api port 
 * --ams.token         : ARGO messaging api token
 * --ams.project       : ARGO messaging api project to connect to
 * --ams.sub           : ARGO messaging subscription to pull from
 * --ams.batch         : num of messages to be retrieved per request to AMS service
 * --ams.interval      : interval (in ms) between AMS service requests
 * --ams.proxy         : optional http proxy url
 * --ams.verify        : optional turn on/off ssl verify
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
		// On failure attempt max 10 times to restart with a retry interval of 2 minutes
		see.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(2, TimeUnit.MINUTES)));

		// Initialize cli parameter tool
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);
		
		// set ams client batch and interval to default values
		int batch = 1;
		long interval = 100L;

		
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
			
			see.getCheckpointConfig().setCheckpointStorage(checkPath);
			// Establish the check-pointing interval
			long checkInt = Long.parseLong(checkInterval);
			see.enableCheckpointing(checkInt);
		}

		// Ingest sync avro encoded data from AMS endpoint
		ArgoMessagingSource ams = new ArgoMessagingSource(endpoint, port, token, project, sub, batch, interval);
		
		if (parameterTool.has("ams.verify")) {
			ams.setVerify(parameterTool.getBoolean("ams.verify"));
		}

		if (parameterTool.has("ams.proxy")) {
			ams.setProxy(parameterTool.get("ams.proxy"));
		}
		
		DataStream<String> metricDataJSON = see.addSource(ams);
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
				
				DatumReader<MetricData> avroReader = new SpecificDatumReader<MetricData>(MetricData.getClassSchema());
				Decoder decoder = DecoderFactory.get().binaryDecoder(decoded64, null);
				
				
				MetricData item;
				try {
				item = avroReader.read(null, decoder);
				} catch (java.io.EOFException ex)
				{
					//convert from old to new
					avroReader = new SpecificDatumReader<MetricData>(MetricDataOld.getClassSchema(),MetricData.getClassSchema());
					decoder = DecoderFactory.get().binaryDecoder(decoded64, null);
					item = avroReader.read(null, decoder);
				}
				if (item != null) {
					LOG.info("Captured data -- {}", item.toString());
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
			
			OutputFileConfig mdataOutputConfig = OutputFileConfig
                    .builder()
                    .withPartPrefix("mdata")
                    .build();
			
			FileSink<MetricData> bs = FileSink.forBulkFormat(new Path(basePath), AvroWriters.forSpecificRecord(MetricData.class))
					.withBucketAssigner(new MsgDateBucketAssigner())
					.withOutputFileConfig(mdataOutputConfig)
					.withBucketCheckInterval(1000) // set the bucket check interval to 1 second
                    .build();
			
			metricDataPOJO.sinkTo(bs);
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
	
	private static class MsgDateBucketAssigner extends BasePathBucketAssigner<MetricData> {

        private static final long serialVersionUID = 8698886003154472344L;

		@Override
        public String getBucketId(MetricData element, Context context) {
            String dailyPart = element.getTimestamp().split("T")[0];
            return  dailyPart;
        }

    }
	
	

}