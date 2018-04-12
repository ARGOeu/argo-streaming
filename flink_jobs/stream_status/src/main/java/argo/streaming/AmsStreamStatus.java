package argo.streaming;


import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.Properties;


import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.codec.binary.Base64;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
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

import argo.avro.GroupEndpoint;
import argo.avro.MetricData;
import argo.avro.MetricProfile;
import status.StatusManager;
import sync.EndpointGroupManagerV2;
import sync.MetricProfileManager;


/**
 * Flink Job : Streaming status computation with multiple destinations (hbase, kafka, fs)
 * job required cli parameters
 * --ams.endpoint      : ARGO messaging api endpoint to connect to msg.example.com
 * --ams.port          : ARGO messaging api port 
 * --ams.token         : ARGO messaging api token
 * --ams.project       : ARGO messaging api project to connect to
 * --ams.sub.metric    : ARGO messaging subscription to pull metric data from
 * --ams.sub.sync      : ARGO messaging subscription to pull sync data from
 * --sync.mps          : metric-profile file used 
 * --sync.egp          : endpoint-group file used for topology
 * --sync.aps          : availability profile used 
 * --sync.ops          : operations profile used
 * Job optional cli parameters:
 * --ams.batch         : num of messages to be retrieved per request to AMS service
 * --ams.interval      : interval (in ms) between AMS service requests
 * --kafka.servers     : list of kafka servers to connect to
 * --kafka.topic       : kafka topic name to publish events
 * --mongo.uri         : mongo uri to store latest status results
 * --mongo.method      : mongo method to use (insert,upsert)
 * --hbase.master      : hbase master hostname
 * --hbase.port        : hbase master.port
 * --hbase.zk.quorum   : hbase zookeeper quorum
 * --hbase.namespace   : hbase namespace
 * --hbase.table       : hbase table name
 * --fs.ouput          : filesystem output path (local or hdfs) mostly for debugging
 * --ams.proxy		   : http proxy url 
 * --timeout           : time in ms - Optional timeout parameter (used in notifications)
 * --daily             : true/false - Optional daily event generation parameter (not needed in notifications)
 */
public class AmsStreamStatus {
	// setup logger
	static Logger LOG = LoggerFactory.getLogger(AmsStreamStatus.class);

	/**
	 * Sets configuration parameters to streaming enviroment
	 * 
	 * @param config
	 *            A StatusConfig object that holds configuration parameters for this
	 *            job
	 * @return Stream execution enviroment
	 */
	private static StreamExecutionEnvironment setupEnvironment(StatusConfig config) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(config.getParameters());

		return env;
	}

	/**
	 * Check if flink job has been called with ams rate params
	 */
	public static boolean hasAmsRateArgs(ParameterTool paramTool) {
		String args[] = { "ams.batch", "ams.interval" };
		return hasArgs(args, paramTool);
	}

	public static boolean hasKafkaArgs(ParameterTool paramTool) {
		String kafkaArgs[] = { "kafka.servers", "kafka.topic" };
		return hasArgs(kafkaArgs, paramTool);
	}

	public static boolean hasHbaseArgs(ParameterTool paramTool) {
		String hbaseArgs[] = { "hbase.master", "hbase.master.port", "hbase.zk.quorum", "hbase.namespace",
				"hbase.table" };
		return hasArgs(hbaseArgs, paramTool);
	}

	public static boolean hasFsOutArgs(ParameterTool paramTool) {
		String fsOutArgs[] = { "fs.output" };
		return hasArgs(fsOutArgs, paramTool);
	}
	
	public static boolean hasMongoArgs(ParameterTool paramTool) {
		String mongoArgs[] = { "mongo.uri", "mongo.method" };
		return hasArgs(mongoArgs, paramTool);
	}

	public static boolean hasArgs(String[] reqArgs, ParameterTool paramTool) {

		for (String reqArg : reqArgs) {
			if (!paramTool.has(reqArg))
				return false;
		}

		return true;
	}

	/**
	 * Main dataflow of flink job
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
		String subMetric = parameterTool.getRequired("ams.sub.metric");
		String subSync = parameterTool.getRequired("ams.sub.sync");

		// set ams client batch and interval to default values
		int batch = 1;
		long interval = 100L;

		if (hasAmsRateArgs(parameterTool)) {
			batch = parameterTool.getInt("ams.batch");
			interval = parameterTool.getLong("ams.interval");
		}
		
		

		// Establish the metric data AMS stream
		// Ingest sync avro encoded data from AMS endpoint
		ArgoMessagingSource amsMetric = new ArgoMessagingSource(endpoint, port, token, project, subMetric, batch, interval);
		ArgoMessagingSource amsSync = new ArgoMessagingSource(endpoint, port, token, project, subSync, batch, interval);

		if (parameterTool.has("ams.verify")) {
			boolean verify = parameterTool.getBoolean("ams.verify");
			amsMetric.setVerify(verify);
			amsSync.setVerify(verify);
		}

		if (parameterTool.has("ams.proxy")) {
			String proxyURL = parameterTool.get("ams.proxy");
			amsMetric.setProxy(proxyURL);
			amsSync.setProxy(proxyURL);
		}
		
		DataStream<String> metricAMS = see.addSource(amsMetric).setParallelism(1);

		// Establish the sync data AMS stream
		DataStream<String> syncAMS = see.addSource(amsSync).setParallelism(1);

		// Forward syncAMS data to two paths
		// - one with parallelism 1 to connect in the first processing step and
		// - one with max parallelism for status event generation step
		// (scalable)
		DataStream<String> syncA = syncAMS.forward();
		DataStream<String> syncB = syncAMS.broadcast();

		DataStream<Tuple2<String, MetricData>> groupMdata = metricAMS.connect(syncA)
				.flatMap(new MetricDataWithGroup(conf)).setParallelism(1);

		DataStream<String> events = groupMdata.connect(syncB).flatMap(new StatusMap(conf));

		events.print();

		if (hasKafkaArgs(parameterTool)) {
			// Initialize kafka parameters
			String kafkaServers = parameterTool.get("kafka.servers");
			String kafkaTopic = parameterTool.get("kafka.topic");
			Properties kafkaProps = new Properties();
			kafkaProps.setProperty("bootstrap.servers", kafkaServers);
			FlinkKafkaProducer09<String> kSink = new FlinkKafkaProducer09<String>(kafkaTopic, new SimpleStringSchema(),
					kafkaProps);
			events.addSink(kSink);
		}

		if (hasHbaseArgs(parameterTool)) {
			// Initialize Output : Hbase Output Format
			HBaseOutputFormat hbf = new HBaseOutputFormat();
			hbf.setMaster(parameterTool.get("hbase.master"));
			hbf.setMasterPort(parameterTool.get("hbase.master.port"));
			hbf.setZkQuorum(parameterTool.get("hbase.zk.quorum"));
			hbf.setZkPort(parameterTool.get("hbase.zk.port"));
			hbf.setNamespace(parameterTool.get("hbase.namespace"));
			hbf.setTableName(parameterTool.get("hbase.table"));
			hbf.setReport(parameterTool.get("report"));
			events.writeUsingOutputFormat(hbf);
		}
		
		if (hasMongoArgs(parameterTool)) {

			MongoStatusOutput mongoOut = new MongoStatusOutput(parameterTool.get("mongo.uri"), "status_metrics",
					"status_endpoints", "status_services", "status_endpoint_groups", parameterTool.get("mongo.method"),
					parameterTool.get("report"));
			events.writeUsingOutputFormat(mongoOut);
		}

		if (hasFsOutArgs(parameterTool)) {
			events.writeAsText(parameterTool.get("fs.output"));
		}

		
		// Create a job title message to discern job in flink dashboard/cli
		StringBuilder jobTitleSB = new StringBuilder();
		jobTitleSB.append("Streaming status using data from ");
		jobTitleSB.append(endpoint);
		jobTitleSB.append(":");
		jobTitleSB.append(port);
		jobTitleSB.append("/v1/projects/");
		jobTitleSB.append(project);
		jobTitleSB.append("/subscriptions/[");
		jobTitleSB.append(subMetric);
		jobTitleSB.append(",");
		jobTitleSB.append(subSync);
		jobTitleSB.append("]");
		
		// Execute flink dataflow
		see.execute();
	}

	/**
	 * MetricDataWithGroup implements a map function that adds group information to
	 * the metric data message
	 */
	private static class MetricDataWithGroup extends RichCoFlatMapFunction<String, String, Tuple2<String, MetricData>> {

		private static final long serialVersionUID = 1L;

		public EndpointGroupManagerV2 egp;
		public MetricProfileManager mps;

		public StatusConfig config;

		public MetricDataWithGroup(StatusConfig config) {
			LOG.info("Created new Status map");
			this.config = config;
		}

		/**
		 * Initializes constructs in the beginning of operation
		 * 
		 * @param parameters
		 *            Configuration parameters to initialize structures
		 * @throws URISyntaxException
		 */
		@Override
		public void open(Configuration parameters) throws IOException, ParseException, URISyntaxException {

			SyncData sd = new SyncData();

			ArrayList<MetricProfile> mpsList = sd.readMetricProfile(config.mps);
			ArrayList<GroupEndpoint> egpList = sd.readGroupEndpoint(config.egp);

			mps = new MetricProfileManager();
			mps.loadFromList(mpsList);
			String validMetricProfile = mps.getProfiles().get(0);
			ArrayList<String> validServices = mps.getProfileServices(validMetricProfile);

			// Trim profile services
			ArrayList<GroupEndpoint> egpTrim = new ArrayList<GroupEndpoint>();
			// Use optimized Endpoint Group Manager
			for (GroupEndpoint egpItem : egpList) {
				if (validServices.contains(egpItem.getService())) {
					egpTrim.add(egpItem);
				}
			}
			egp = new EndpointGroupManagerV2();
			egp.loadFromList(egpTrim);

		}

		/**
		 * The main flat map function that accepts metric data and generates metric data
		 * with group information
		 * 
		 * @param value
		 *            Input metric data in base64 encoded format from AMS service
		 * @param out
		 *            Collection of generated Tuple2<MetricData,String> objects
		 */
		@Override
		public void flatMap1(String value, Collector<Tuple2<String, MetricData>> out)
				throws IOException, ParseException {

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

			//System.out.println("metric data item received" + item.toString());

			// generate events and get them
			String service = item.getService();
			String hostname = item.getHostname();

			ArrayList<String> groups = egp.getGroup(hostname, service);
			//System.out.println(egp.getList());

			for (String groupItem : groups) {
				Tuple2<String, MetricData> curItem = new Tuple2<String, MetricData>();
				curItem.f0 = groupItem;
				curItem.f1 = item;
				out.collect(curItem);
				//System.out.println("item enriched: " + curItem.toString());
			}

		}

		public void flatMap2(String value, Collector<Tuple2<String, MetricData>> out)
				throws IOException, ParseException {

			JsonParser jsonParser = new JsonParser();
			// parse the json root object
			JsonElement jRoot = jsonParser.parse(value);
			// parse the json field "data" and read it as string
			// this is the base64 string payload
			String data = jRoot.getAsJsonObject().get("data").getAsString();
			// Decode from base64
			byte[] decoded64 = Base64.decodeBase64(data.getBytes("UTF-8"));
			JsonElement jAttr = jRoot.getAsJsonObject().get("attributes");
			Map<String, String> attr = SyncParse.parseAttributes(jAttr);
			if (attr.containsKey("type")) {

				String sType = attr.get("type");
				if (sType.equalsIgnoreCase("metric_profile")) {
					// Update mps
					ArrayList<MetricProfile> mpsList = SyncParse.parseMetricProfile(decoded64);
					mps = new MetricProfileManager();
					mps.loadFromList(mpsList);
				} else if (sType.equals("group_endpoint")) {
					// Update egp
					ArrayList<GroupEndpoint> egpList = SyncParse.parseGroupEndpoint(decoded64);
					egp = new EndpointGroupManagerV2();

					String validMetricProfile = mps.getProfiles().get(0);
					ArrayList<String> validServices = mps.getProfileServices(validMetricProfile);
					// Trim profile services
					ArrayList<GroupEndpoint> egpTrim = new ArrayList<GroupEndpoint>();
					// Use optimized Endpoint Group Manager
					for (GroupEndpoint egpItem : egpList) {
						if (validServices.contains(egpItem.getService())) {
							egpTrim.add(egpItem);
						}
					}
				}
			}

		}

	}

	/**
	 * StatusMap implements a rich flat map function which holds status information
	 * for all entities in topology and for each received metric generates the
	 * appropriate status events
	 */
	private static class StatusMap extends RichCoFlatMapFunction<Tuple2<String, MetricData>, String, String> {

		private static final long serialVersionUID = 1L;

		private String pID;

		public StatusManager sm;

		public StatusConfig config;

		public int defStatus;
		
		

		public StatusMap(StatusConfig config) {
			LOG.info("Created new Status map");
			this.config = config;
		}

		/**
		 * Initializes constructs in the beginning of operation
		 * 
		 * @param parameters
		 *            Configuration parameters to initialize structures
		 * @throws URISyntaxException
		 */
		@Override
		public void open(Configuration parameters) throws IOException, ParseException, URISyntaxException {

			pID = Integer.toString(getRuntimeContext().getIndexOfThisSubtask());
			
			SyncData sd = new SyncData();

			String opsJSON = sd.readText(config.ops);
			String apsJSON = sd.readText(config.aps);
			ArrayList<MetricProfile> mpsList = sd.readMetricProfile(config.mps);
			ArrayList<GroupEndpoint> egpListFull = sd.readGroupEndpoint(config.egp);

			// create a new status manager
			sm = new StatusManager();
			sm.setTimeout(config.timeout);
			// load all the connector data
			sm.loadAll(egpListFull, mpsList, apsJSON, opsJSON);

			// Set the default status as integer
			defStatus = sm.getOps().getIntStatus(config.defStatus);
			LOG.info("Initialized status manager:" + pID + " (with timeout:" + sm.getTimeout() + ")");

		}

		/**
		 * The main flat map function that accepts metric data and generates status
		 * events
		 * 
		 * @param value
		 *            Input metric data in base64 encoded format from AMS service
		 * @param out
		 *            Collection of generated status events as json strings
		 */
		@Override
		public void flatMap1(Tuple2<String, MetricData> value, Collector<String> out)
				throws IOException, ParseException {

			MetricData item = value.f1;
			String group = value.f0;

			String service = item.getService();
			String hostname = item.getHostname();
			String metric = item.getMetric();
			String status = item.getStatus();
			String tsMon = item.getTimestamp();
			String monHost = item.getMonitoringHost();
			String message = item.getMessage();
			String summary = item.getSummary();

			// if daily generation is enable check if has day changed?
			if (config.daily && sm.hasDayChanged(sm.getTsLatest(), tsMon)) {
				ArrayList<String> eventsDaily = sm.dumpStatus(tsMon);
				sm.setTsLatest(tsMon);
				for (String event : eventsDaily) {
					out.collect(event);
					LOG.info("sm-" + pID + ": daily event produced: " + event);
				}
			}

			// check if group is handled by this operator instance - if not
			// construct the group based on sync data
			if (!sm.hasGroup(group)) {
				// Get start of the day to create new entries
				Date dateTS = sm.setDate(tsMon);
				sm.addGroup(group, service, hostname, defStatus, dateTS);
			}

			ArrayList<String> events = sm.setStatus(service, hostname, metric, status, monHost, tsMon, message, summary);

			for (String event : events) {
				out.collect(event);
				LOG.info("sm-" + pID + ": event produced: " + item);
			}
		}

		public void flatMap2(String value, Collector<String> out) throws IOException, ParseException {

			JsonParser jsonParser = new JsonParser();
			// parse the json root object
			JsonElement jRoot = jsonParser.parse(value);
			// parse the json field "data" and read it as string
			// this is the base64 string payload
			String data = jRoot.getAsJsonObject().get("data").getAsString();
			// Decode from base64
			byte[] decoded64 = Base64.decodeBase64(data.getBytes("UTF-8"));
			JsonElement jAttr = jRoot.getAsJsonObject().get("attributes");
			Map<String, String> attr = SyncParse.parseAttributes(jAttr);
			if (attr.containsKey("type")) {

				String sType = attr.get("type");
				if (sType.equalsIgnoreCase("metric_profile")) {
					// Update mps
					ArrayList<MetricProfile> mpsList = SyncParse.parseMetricProfile(decoded64);
					sm.mps = new MetricProfileManager();
					sm.mps.loadFromList(mpsList);
				} else if (sType.equals("group_endpoint")) {
					// Update egp
					ArrayList<GroupEndpoint> egpList = SyncParse.parseGroupEndpoint(decoded64);

					String validMetricProfile = sm.mps.getProfiles().get(0);
					ArrayList<String> validServices = sm.mps.getProfileServices(validMetricProfile);
					// Trim profile services
					ArrayList<GroupEndpoint> egpTrim = new ArrayList<GroupEndpoint>();
					// Use optimized Endpoint Group Manager
					for (GroupEndpoint egpItem : egpList) {
						if (validServices.contains(egpItem.getService())) {
							egpTrim.add(egpItem);
						}
					}

					sm.egp = new EndpointGroupManagerV2();
					sm.egp.loadFromList(egpTrim);
				}
			}

		}

	}

	/**
	 * HbaseOutputFormat implements a custom output format for storing results in
	 * hbase
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

		public void setReport(String report) {
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
		 * @parameter record A string with json represantation of a status event
		 */
		@Override
		public void writeRecord(String record) throws IOException {

			JsonParser jsonParser = new JsonParser();
			// parse the json root object
			JsonObject jRoot = jsonParser.parse(record).getAsJsonObject();
			// Get fields

			String rep = this.report;
			String tp = extractJson("type", jRoot);
			String dt = extractJson("date", jRoot);
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
			// report > metric_type > date(day) > endpoint group > service >
			// hostname > metric
			String key = rep + "|" + tp + "|" + dt + "|" + eGroup + "|" + service + "|" + hostname + "|" + metric + "|"
					+ tsm;

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