package argo.streaming;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink Streaming JOB for Ingesting Sync Data to HDFS
 * job required cli parameters:
 * --ams.endpoint     : ARGO messaging api endoint to connect to msg.example.com
 * --ams.port          : ARGO messaging api port 
 * --ams.token         : ARGO messaging api token
 * --ams.project       : ARGO messaging api project to connect to
 * --ams.sub.metric    : ARGO messaging subscription to pull metric data from
 * --ams.sub.sync      : ARGO messaging subscription to pull sync data from
 * --hdfs.path         : Hdfs destination path to store the data
 * --ams.batch         : num of messages to be retrieved per request to AMS service
 * --ams.interval      : interval (in ms) between AMS service requests
 * --ams.proxy         : optional http proxy url
 * --ams.verify        : optional turn on/off ssl verify
 */
public class AmsIngestSync {

	// setup logger
	static Logger LOG = LoggerFactory.getLogger(AmsIngestSync.class);

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
	
	/**
	 * Check if flink job has been called with ams rate params
	 */
	public static boolean hasAmsRateArgs(ParameterTool paramTool) {
		String args[] = { "ams.batch", "ams.interval" };
		return hasArgs(args, paramTool);
	}

	// main job function
	public static void main(String[] args) throws Exception {

		// Create flink execution enviroment
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setParallelism(1);
		// Fixed restart strategy: on failure attempt max 10 times to restart with a retry interval of 2 minutes
		see.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(2, TimeUnit.MINUTES)));
		// Initialize cli parameter tool
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		// Initialize Input Source : ARGO Messaging Source
		String endpoint = parameterTool.getRequired("ams.endpoint");
		String port = parameterTool.getRequired("ams.port");
		String token = parameterTool.getRequired("ams.token");
		String project = parameterTool.getRequired("ams.project");
		String sub = parameterTool.getRequired("ams.sub");
		String basePath = parameterTool.getRequired("hdfs.path");

		// set ams client batch and interval to default values
		int batch = 1;
		long interval = 100L;

		if (hasAmsRateArgs(parameterTool)) {
			batch = parameterTool.getInt("ams.batch");
			interval = parameterTool.getLong("ams.interval");
		}

		
		//Ingest sync avro encoded data from AMS endpoint
		ArgoMessagingSource ams = new ArgoMessagingSource(endpoint, port, token, project, sub, batch, interval);

		if (parameterTool.has("ams.verify")){
			ams.setVerify(parameterTool.getBoolean("ams.verify"));
		}

		if (parameterTool.has("ams.proxy")) {
			ams.setProxy(parameterTool.get("ams.proxy"));
		}
		DataStream<String> syncDataStream = see
				.addSource(ams);

		SyncHDFSOutputFormat hdfsOut = new SyncHDFSOutputFormat();
		hdfsOut.setBasePath(basePath);

		syncDataStream.writeUsingOutputFormat(hdfsOut);
		
		// Create a job title message to discern job in flink dashboard/cli
		StringBuilder jobTitleSB = new StringBuilder();
		jobTitleSB.append("Ingesting sync data from ");
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
