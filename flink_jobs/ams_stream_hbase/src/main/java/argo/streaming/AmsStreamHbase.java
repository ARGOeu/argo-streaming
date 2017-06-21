package argo.streaming;

import java.io.File;
import java.io.IOException;

import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.codec.binary.Base64;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.RollingSink;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import argo.avro.MetricData;


//Flink Job : Stream metric data from ARGO messaging to Hbase
//job required cli parameters
//--ams-endpoint      : ARGO messaging api endoint to connect to msg.example.com
//--ams-port          : ARGO messaging api port 
//--ams-token         : ARGO messaging api token
//--ams-project       : ARGO messaging api project to connect to
//--ams-sub           : ARGO messaging subscription to pull from
//--hbase-master      : hbase endpoint
//--hbase-master-port : hbase master port
//--hbase-zk-quorum   : comma separated list of hbase zookeeper servers
//--hbase-zk-port     : port used by hbase zookeeper servers
//--hbase-namespace   : table namespace used (usually tenant name)
//--hbase-table       : table name (usually metric_data)
//--hdfs              : hdfs destination to write the data
public class AmsStreamHbase {
	// setup logger
	static Logger LOG = LoggerFactory.getLogger(AmsStreamHbase.class);

	public static void main(String[] args) throws Exception {

		// Create flink execution enviroment
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

		// Initialize cli parameter tool
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		// Initialize Input Source : ARGO Messaging Source
		String endpoint = parameterTool.getRequired("ams-endpoint");
		String port = parameterTool.getRequired("ams-port");
		String token = parameterTool.getRequired("ams-token");
		String project = parameterTool.getRequired("ams-project");
		String sub = parameterTool.getRequired("ams-sub");
		DataStream<String> messageStream = see.addSource(new ArgoMessagingSource(endpoint, port, token, project, sub));

		// Initialize Output : Hbase Output Format
		HBaseOutputFormat hbf = new HBaseOutputFormat();
		hbf.setMaster(parameterTool.getRequired("hbase-master"));
		hbf.setMasterPort(parameterTool.getRequired("hbase-master-port"));
		hbf.setZkQuorum(parameterTool.getRequired("hbase-zk-quorum"));
		hbf.setZkPort(parameterTool.getRequired("hbase-zk-port"));
		hbf.setNamespace(parameterTool.getRequired("hbase-namespace"));
		hbf.setTableName(parameterTool.getRequired("hbase-table"));

		RollingSink<MetricData> rSink = new RollingSink<MetricData>(parameterTool.getRequired("hdfs"));
	    rSink.setBatchSize(1024);
	   
		
		// Intermediate Transformation
		// Map function: json msg -> payload -> base64decode -> avrodecode ->
		// hbase
		SingleOutputStreamOperator<String> msgStream = messageStream.rebalance().map(new MapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String map(String value) throws IOException {
				JsonParser jsonParser = new JsonParser();
				// parse the json root object
				JsonElement jRoot = jsonParser.parse(value);
				// parse the json field "data" and read it as string
				// this is the base64 string payload
				String data = jRoot.getAsJsonObject().get("data").getAsString();
				// Decode from base64
				byte[] decoded64 = Base64.decodeBase64(data.getBytes("UTF-8"));
				// Decode from avro
				Schema avroSchema = new Schema.Parser().parse(new File(parameterTool.getRequired("avro-schema")));
				DatumReader<GenericRecord> avroReader = new SpecificDatumReader<GenericRecord>(avroSchema);
				Decoder decoder = DecoderFactory.get().binaryDecoder(decoded64, null);
				GenericRecord payload2;
				payload2 = avroReader.read(null, decoder);
				// If not avro return the string
				if (payload2 != null) {
					return payload2.toString();
				} else {
					return Arrays.toString(decoded64);
				}
			}

		});
		
		msgStream.writeUsingOutputFormat(hbf);
	

		// Execute flink dataflow
		see.execute();
	}

	// Hbase output format
	private static class HBaseOutputFormat implements OutputFormat<String> {

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

		@Override
		public void configure(Configuration parameters) {

		}

		@Override
		public void open(int taskNumber, int numTasks) throws IOException {
			// Create hadoop based configuration for hclient to use
			org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
			// Modify configuration to job needs
			config.setInt("timeout", 120000);
			if (masterPort != null &&  !masterPort.isEmpty()){
				config.set("hbase.master", master + ":" + masterPort);
			}else {
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

		private String extractJson(String field, JsonObject root){
			JsonElement el = root.get(field);
			if (el!=null && !(el.isJsonNull())){
			
				return el.getAsString();
				
			}
			return "";
		}
		
		
		@Override
		public void writeRecord(String record) throws IOException  {
				
				JsonParser jsonParser = new JsonParser();
				// parse the json root object
				JsonObject jRoot = jsonParser.parse(record).getAsJsonObject();
				// Get fields
				String ts = extractJson("timestamp",jRoot);
				String host = extractJson("hostname",jRoot);
				String service = extractJson("service",jRoot);
				String metric =extractJson("metric",jRoot);
				String mHost = extractJson("monitoring_host",jRoot);
				String status = extractJson("status",jRoot);
				String summary = extractJson("summary",jRoot);
				String msg = extractJson("message",jRoot);
				String tags = extractJson("tags",jRoot);

				// Compile key
				String key =  host + "|" + service + "|" + metric + "|" +ts+ "|" + mHost;

				// Prepare columns
				Put put = new Put(Bytes.toBytes(key));
				put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("timestamp"), Bytes.toBytes(ts));
				put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("host"), Bytes.toBytes(host));
				put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("service"), Bytes.toBytes(service));
				put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("metric"), Bytes.toBytes(metric));
				put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("monitoring_host"), Bytes.toBytes(mHost));
				put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("status"), Bytes.toBytes(status));
				put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("summary"), Bytes.toBytes(summary));
				put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("msg"), Bytes.toBytes(msg));
				put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("tags"), Bytes.toBytes(tags));

				// Insert row in hbase
				ht.put(put);
				
			
			
		

		}

		@Override
		public void close() throws IOException {
			ht.close();
			connection.close();
		}

	}
}