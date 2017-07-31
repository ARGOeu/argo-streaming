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
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import status.StatusManager;

//Flink Job : Stream metric data from ARGO messaging to Hbase
//job required cli parameters
//--ams.endpoint      : ARGO messaging api endoint to connect to msg.example.com
//--ams.port          : ARGO messaging api port 
//--ams.token         : ARGO messaging api token
//--ams.project       : ARGO messaging api project to connect to
//--ams.sub           : ARGO messaging subscription to pull from
//--avro.schema       : avro-schema used for decoding payloads
//--sync.mps          : metric-profile file used 
//--sync.egp          : endpoint-group file used for topology
//--sync.aps          : availability profile used 
//--sync.ops          : operations profile used 


/**
 *  Represents an ARGO AMS Streaming job in flink
 */
public class AmsStreamStatus {
	// setup logger
	static Logger LOG = LoggerFactory.getLogger(AmsStreamStatus.class);

	
	/**
	 *  Sets configuration parameters to streaming enviroment
	 *  
	 *  @param  config  A StatusConfig object that holds configuration parameters for this job
	 *  @return         Stream execution enviroment       
	 */
	private static StreamExecutionEnvironment setupEnvironment(StatusConfig config) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(config.getParameters());

		return env;
	}

	/**
	 *  Main dataflow of flink job     
	 */
	public static void main(String[] args) throws Exception {

	

		// Initialize cli parameter tool
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		final StatusConfig conf = new StatusConfig(parameterTool);

		StreamExecutionEnvironment see = setupEnvironment(conf);

		
		see.setParallelism(1);
		// Initialize Input Source : ARGO Messaging Source
		String endpoint = parameterTool.getRequired("ams.endpoint");
		String port = parameterTool.getRequired("ams.port");
		String token = parameterTool.getRequired("ams.token");
		String project = parameterTool.getRequired("ams.project");
		String sub = parameterTool.getRequired("ams.sub");
		
		String report = parameterTool.getRequired("report");
		
		// Initialize Output : Hbase Output Format
		HBaseOutputFormat hbf = new HBaseOutputFormat();
		hbf.setMaster(parameterTool.getRequired("hbase.master"));
		hbf.setMasterPort(parameterTool.getRequired("hbase.master.port"));
		hbf.setZkQuorum(parameterTool.getRequired("hbase.zk.quorum"));
		hbf.setZkPort(parameterTool.getRequired("hbase.zk.port"));
		hbf.setNamespace(parameterTool.getRequired("hbase.namespace"));
		hbf.setTableName(parameterTool.getRequired("hbase.table"));
		hbf.setReport(parameterTool.getRequired("report"));

		DataStream<String> messageStream = see.addSource(new ArgoMessagingSource(endpoint, port, token, project, sub));

		// Intermediate Transformation
		// Map function: json msg -> payload -> base64decode -> avrodecode ->
		// kafka
		messageStream.flatMap(new StatusMap(conf)).writeUsingOutputFormat(hbf);

		// Execute flink dataflow
		see.execute();
	}

	/**
	 *  StatusMap implements a rich flat map fucntion which holds status information
	 *  for all entities in topology and for each received metric generates the 
	 *  appropriate status events   
	 */
	private static class StatusMap extends RichFlatMapFunction<String, String> {

		private static final long serialVersionUID = 1L;

		public StatusManager sm;

		public StatusConfig config;

		public StatusMap(StatusConfig config) {
			LOG.info("Created new Status map");
			this.config = config;
		}

		/**
		 *  Initializes constructs in the beginning of operation
		 *  
		 *  @param parameters Configuration parameters to initialize structures
		 */
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
			if (config.runDate.equals("")) {
				sm.construct(sm.getOps().getIntStatus("MISSING"), sm.getToday());
			} else {
				sm.construct(sm.getOps().getIntStatus("MISSING"), sm.setDate(config.runDate));
			}

		}

		/**
		 *  The main flat map function that accepts metric data and generates 
		 *  status events 
		 *  
		 *  @param  value   Input metric data in base64 encoded format from AMS service
		 *  @param  out     Collection of generated status events as json strings    
		 */
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
			String tsMon = pr.get("timestamp").toString();
			String monHost = pr.get("monitoring_host").toString();
			
			// Check if this is the first time starting and generate initial events
			if (sm.getFirstGen()){
				ArrayList<String> eventsInit = sm.dumpStatus(tsMon);
				sm.disableFirstGen();
				for (String item : eventsInit)
				{
					out.collect(item);
					LOG.info("initial event produced: " + item);
				}
				
			}
			
			// Has Date Changed?
			if (sm.hasDayChanged(sm.getTsLatest(), tsMon)){
				ArrayList<String> eventsDaily = sm.dumpStatus(tsMon);
				sm.setTsLatest(tsMon);
				for (String item : eventsDaily)
				{
					out.collect(item);
					LOG.info("daily event produced: " + item);
				}
			}
			
			ArrayList<String> events = sm.setStatus(service, hostname, metric, status, monHost, tsMon);

			for (String item : events) {
				out.collect(item);
				LOG.info("event produced: " + item);
			}
		}

	}

	/**
	 *  HbaseOutputFormat implements a custom output format for storing results in hbase 
	 */
	private static class HBaseOutputFormat implements OutputFormat<String> {

		private String report = null;
		private String master = null;
		private String masterPort = null;
		private String zkQuorum = null;
		private String zkPort = null;
		private String namespace = null;
		private String tname = null;
		private Connection connection = null;
		private Table ht = null;

		private static final long serialVersionUID = 1L;

		
		// Setters
		public void setMasterPort(String masterPort) {
			this.masterPort = masterPort;
		}
		
		public void setMaster(String master) {
			this.master = master;
		}

		public void setZkQuorum(String zkQuorum) {
			this.zkQuorum = zkQuorum;
		}

		public void setZkPort(String zkPort) {
			this.zkPort = zkPort;
		}

		public void setNamespace(String namespace) {
			this.namespace = namespace;
		}

		public void setTableName(String tname) {
			this.tname = tname;
		}
		
		public void setReport(String report){
			this.report = report;
		}

		@Override
		public void configure(Configuration parameters) {

		}

	
		/**
		 * Structure initialization
		 */
		@Override
		public void open(int taskNumber, int numTasks) throws IOException {
			// Create hadoop based configuration for hclient to use
			org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
			// Modify configuration to job needs
			config.setInt("timeout", 120000);
			if (masterPort != null && !masterPort.isEmpty()) {
				config.set("hbase.master", master + ":" + masterPort);
			} else {
				config.set("hbase.master", master + ":60000");
			}

			config.set("hbase.zookeeper.quorum", zkQuorum);
			config.set("hbase.zookeeper.property.clientPort", (zkPort));
			// Create the connection
			connection = ConnectionFactory.createConnection(config);
			if (namespace != null) {
				ht = connection.getTable(TableName.valueOf(namespace + ":" + tname));
			} else {
				ht = connection.getTable(TableName.valueOf(tname));
			}

		}
		
		/**
		 * Extract json representation as string to be used as a field value
		 */
		private String extractJson(String field, JsonObject root) {
			JsonElement el = root.get(field);
			if (el != null && !(el.isJsonNull())) {

				return el.getAsString();

			}
			return "";
		}


		/**
		 * Accepts status event as json string and stores it in hbase table
		 * 
		 * @parameter   record   A string with json represantation of a status event
		 */
		@Override
		public void writeRecord(String record) throws IOException {

			JsonParser jsonParser = new JsonParser();
			// parse the json root object
			JsonObject jRoot = jsonParser.parse(record).getAsJsonObject();
			// Get fields

			String rep = this.report;
			String tp = extractJson("type", jRoot);
			String dt = extractJson("date",jRoot);
			String eGroup = extractJson("endpoint_group", jRoot);
			String service = extractJson("service", jRoot);
			String hostname = extractJson("hostname", jRoot);
			String metric = extractJson("metric", jRoot);
			String status = extractJson("status", jRoot);
			String prevStatus = extractJson("prev_status", jRoot);
			String prevTs = extractJson("prev_ts", jRoot);
			String tsm = extractJson("ts_monitored", jRoot);
			String tsp = extractJson("ts_processed", jRoot);

			// Compile key
			// Key is constructed based on 
			// report > metric_type > date(day) > endpoint group > service > hostname > metric
			String key = rep + "|" + tp + "|" + dt + "|" + eGroup + "|" + service + "|" + hostname + "|" + metric + "|" + tsm;

			// Prepare columns
			Put put = new Put(Bytes.toBytes(key));
			put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("report"), Bytes.toBytes(rep));
			put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("type"), Bytes.toBytes(tp));
			put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("endpoint_group"), Bytes.toBytes(eGroup));
			put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("service"), Bytes.toBytes(service));
			put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("hostname"), Bytes.toBytes(hostname));
			put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("metric"), Bytes.toBytes(metric));
			put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("status"), Bytes.toBytes(status));
			put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("prev_status"), Bytes.toBytes(prevStatus));
			put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("prev_ts"), Bytes.toBytes(prevTs));
			put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("ts_monitored"), Bytes.toBytes(tsm));
			put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("ts_processed"), Bytes.toBytes(tsp));

			// Insert row in hbase
			ht.put(put);

		}

		/**
		 * Closes hbase table and hbase connection  
		 */
		@Override
		public void close() throws IOException {
			ht.close();
			connection.close();
		}
	}

}