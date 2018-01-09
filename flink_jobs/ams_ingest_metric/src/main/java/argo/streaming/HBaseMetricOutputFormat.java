package argo.streaming;

import java.io.IOException;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import argo.avro.MetricData;

/**
 * Hbase Output Format for storing Metric Data to an hbase destination
 */
public class HBaseMetricOutputFormat implements OutputFormat<MetricData> {

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

	/**
	 * Initialize Hbase remote connection
	 */
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

	/**
	 * Store a Metric Data object as an Hbase Record
	 */
	@Override
	public void writeRecord(MetricData record) throws IOException  {
			
			String ts = record.getTimestamp();
			String host = record.getHostname();
			String service = record.getService();
			String metric = record.getMetric();
			String mHost = record.getMonitoringHost();
			String status = record.getStatus();
			String summary = record.getSummary();
			String msg = record.getMessage();
			String tags = record.getTags().toString();

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

	/**
	 * Close Hbase Connection
	 */
	@Override
	public void close() throws IOException {
		ht.close();
		connection.close();
	}

}