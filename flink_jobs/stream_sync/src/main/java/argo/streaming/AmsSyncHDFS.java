package argo.streaming;


import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Flink Streaming JOB for Ingesting Sync Data to HDFS
 */
public class AmsSyncHDFS {

	// setup logger
	static Logger LOG = LoggerFactory.getLogger(AmsSyncHDFS.class);

	// main job function
	public static void main(String[] args) throws Exception {

		// Create flink execution enviroment
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setParallelism(1);
		// Initialize cli parameter tool
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		// Initialize Input Source : ARGO Messaging Source
		String endpoint = parameterTool.getRequired("ams.endpoint");
		String port = parameterTool.getRequired("ams.port");
		String token = parameterTool.getRequired("ams.token");
		String project = parameterTool.getRequired("ams.project");
		String sub = parameterTool.getRequired("ams.sub");
		String basePath = parameterTool.getRequired("base.path");
		
		// Ingest sync avro encoded data from AMS endpoint
		DataStream<String> syncDataStream = see.addSource(new ArgoMessagingSource(endpoint, port, token, project, sub));

		SyncHDFSOutputFormat hdfsOut = new SyncHDFSOutputFormat();
		hdfsOut.setBasePath(basePath);
		
		syncDataStream.writeUsingOutputFormat(hdfsOut);
		
		see.execute();
		
	}

}
