package argo.streaming;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.codec.binary.Base64;

import org.apache.flink.api.common.functions.RichFlatMapFunction;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;

import com.google.gson.JsonParser;

import status.StatusManager;

//Flink Job : Stream metric data from ARGO messaging to Hbase
//job required cli parameters
//--ams.endpoint      : ARGO messaging api endoint to connect to msg.example.com
//--ams.port          : ARGO messaging api port 
//--ams.token         : ARGO messaging api token
//--ams.project       : ARGO messaging api project to connect to
//--ams.sub           : ARGO messaging subscription to pull from
//--kafka.list        : kafka broker list
//--kafka.topic       : kafka topic
//--avro.schema       : avro-schema used for decoding payloads
//--sync.mps          : metric-profile file used 
//--sync.egp          : endpoint-group file used for topology
//--sync.aps          : availability profile used 
//--sync.ops          : operations profile used 
public class AmsStreamStatus {
	// setup logger
	static Logger LOG = LoggerFactory.getLogger(AmsStreamStatus.class);

	private static StreamExecutionEnvironment setupEnvironment(StatusConfig config) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(config.getParameters());

		return env;
	}

	public static void main(String[] args) throws Exception {

		// // Create flink execution enviroment
		// StreamExecutionEnvironment see =
		// StreamExecutionEnvironment.getExecutionEnvironment();

		// Initialize cli parameter tool
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		final StatusConfig conf = new StatusConfig(parameterTool);

		StreamExecutionEnvironment see = setupEnvironment(conf);
		

		// Initialize Input Source : ARGO Messaging Source
		String endpoint = parameterTool.getRequired("ams.endpoint");
		String port = parameterTool.getRequired("ams.port");
		String token = parameterTool.getRequired("ams.token");
		String project = parameterTool.getRequired("ams.project");
		String sub = parameterTool.getRequired("ams.sub");
		String brList = parameterTool.getRequired("kafka.hosts");
		String brTopic = parameterTool.getRequired("kafka.topic");
		DataStream<String> messageStream = see.addSource(new ArgoMessagingSource(endpoint, port, token, project, sub));

		FlinkKafkaProducer09<String> myProducer = new FlinkKafkaProducer09<String>(brList, // broker
																							// list
				brTopic, // target topic
				new SimpleStringSchema()); // serialization schema

		// Intermediate Transformation
		// Map function: json msg -> payload -> base64decode -> avrodecode ->
		// kafka
		messageStream.flatMap(new StatusMap(conf)).addSink(myProducer);

		// Execute flink dataflow
		see.execute();
	}

	private static class StatusMap extends RichFlatMapFunction<String, String> {

		private static final long serialVersionUID = 1L;

		public StatusManager sm;

		public StatusConfig config;

		public StatusMap(StatusConfig config) {
			LOG.info("--- Created new Status map");
			this.config = config;
		}

		@Override
		public void open(Configuration parameters) throws IOException, ParseException {
			LOG.info("initializing status manager");
			File opsF = new File(config.ops);
			File apsF = new File(config.aps);
			File mpsF = new File(config.mps);
			File egpF = new File(config.egp);

			sm = new StatusManager();
			sm.loadAll(egpF, mpsF, apsF, opsF);
			
			// Get runDate parameter
			if (config.runDate.equals("")){
				sm.construct(sm.getOps().getIntStatus("OK"), sm.getToday());
			}else {
				sm.construct(sm.getOps().getIntStatus("OK"), sm.setDate(config.runDate));
			}
			
			
		}

		@Override
		public void flatMap(String value, Collector<String> out) throws IOException, ParseException {

			JsonParser jsonParser = new JsonParser();
			// parse the json root object
			JsonElement jRoot = jsonParser.parse(value);
			// parse the json field "data" and read it as string
			// this is the base64 string payload
			String data = jRoot.getAsJsonObject().get("data").getAsString();
			// Decode from base64
			byte[] decoded64 = Base64.decodeBase64(data.getBytes("UTF-8"));
			// Decode from avro
			LOG.info(config.avroSchema);
			Schema avroSchema = new Schema.Parser().parse(new File(config.avroSchema));
			DatumReader<GenericRecord> avroReader = new SpecificDatumReader<GenericRecord>(avroSchema);
			Decoder decoder = DecoderFactory.get().binaryDecoder(decoded64, null);
			GenericRecord pr;
			pr = avroReader.read(null, decoder);

			// generate events and get them
			String service = pr.get("service").toString();
			String hostname = pr.get("hostname").toString();
			String metric = pr.get("metric").toString();
			String status = pr.get("status").toString();
			String timestamp = pr.get("timestamp").toString();

			ArrayList<String> events = sm.setStatus(pr.get("service").toString(), pr.get("hostname").toString(),
					pr.get("metric").toString(), pr.get("status").toString(), pr.get("timestamp").toString());

			for (String item : events) {
				out.collect(item);
				LOG.info("event produced: " + item);
			}
		}

	}

}