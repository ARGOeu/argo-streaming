package argo.streaming;

import java.io.IOException;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;

import status.StatusEvent;


/**
 * MongoOutputFormat for storing status data to mongodb
 */
public class MongoStatusOutput implements OutputFormat<String> {

	public enum MongoMethod {
		INSERT, UPSERT
	};

	
	private static final long serialVersionUID = 1L;

	private String mongoHost;
	private int mongoPort;
	private String dbName;
	private String metricName;
	private String endpointName;
	private String serviceName;
	private String egroupName;
	private MongoMethod method;
	private String report;

	private MongoClient mClient;
	private MongoDatabase mDB;
	private MongoCollection<Document> metricCol;
	private MongoCollection<Document> endpointCol;
	private MongoCollection<Document> serviceCol;
	private MongoCollection<Document> egroupCol;

	// constructor
	public MongoStatusOutput(String uri, String metricName,String serviceName, String endpointName, String egroupName, String method , String report) {

		if (method.equalsIgnoreCase("upsert")) {
			this.method = MongoMethod.UPSERT;
		} else {
			this.method = MongoMethod.INSERT;
		}

		
		this.report = report;

		MongoClientURI mURI = new MongoClientURI(uri);
		String[] hostParts = mURI.getHosts().get(0).split(":");
		String hostname = hostParts[0];
		int port = Integer.parseInt(hostParts[1]);

		this.mongoHost = hostname;
		this.mongoPort = port;
		this.dbName = mURI.getDatabase();
		this.metricName = metricName;
		this.serviceName = serviceName;
		this.endpointName = endpointName;
		this.egroupName = egroupName;
	}

	// constructor
	public MongoStatusOutput(String host, int port, String db, String metricName,String serviceName, String endpointName, String egroupName, MongoMethod method, 
			String report) {
		this.mongoHost = host;
		this.mongoPort = port;
		this.dbName = db;
		this.metricName = metricName;
		this.serviceName = serviceName;
		this.endpointName = endpointName;
		this.egroupName = egroupName;
		this.method = method;
		this.report = report;
	}

	private void initMongo() {
		this.mClient = new MongoClient(mongoHost, mongoPort);
		this.mDB = mClient.getDatabase(dbName);
		this.metricCol = mDB.getCollection(metricName);
		this.endpointCol = mDB.getCollection(endpointName);
		this.serviceCol = mDB.getCollection(serviceName);
		this.egroupCol = mDB.getCollection(egroupName);
	}

	/**
	 * Initialize MongoDB remote connection
	 */
	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		// Configure mongo
		initMongo();
	}

	/**
	 * Prepare correct MongoDocument according to record values and selected StatusType.
	 * A different document is needed for storing Status Metric results than Endpoint,
	 * Service or Endpoint Group ones.       
	 */
	private Document prepDoc(StatusEvent record) {
		Document doc = new Document("report",this.report)
				.append("endpoint_group", record.getGroup());
				
		
		if (record.getType().equalsIgnoreCase("service")) {
			
			doc.append("service",record.getService());
		
		} else if (record.getType().equalsIgnoreCase("endpoint")) {
			
			doc.append("service", record.getService())
			.append("host", record.getHostname());
				
		} else if (record.getType().equalsIgnoreCase("metric")) {
		
			doc.append("service", record.getService())
			.append("host", record.getHostname())
			.append("metric", record.getMetric())
			.append("message", record.getMessage())
			.append("summary", record.getSummary())
			.append("time_integer",record.getTimeInt()) 
			.append("previous_state",record.getPrevStatus())
			.append("previous_ts", record.getPrevTs());
		}
		
		
		doc.append("status",record.getStatus())
				.append("timestamp",record.getTsMonitored())
				.append("date_integer",record.getDateInt());
		
		return doc;
	}
	
	/**
	 * Prepare correct Update filter according to record values and selected StatusType.
	 * A different update filter is needed for updating Status Metric results than Endpoint,
	 * Service or Endpoint Group ones.       
	 */
	private Bson prepFilter(StatusEvent record) {
	
		if (record.getType().equalsIgnoreCase("metric")) {
			
			return Filters.and(Filters.eq("report", this.report), Filters.eq("date_integer", record.getDateInt()),
					Filters.eq("endpoint_group", record.getGroup()), Filters.eq("service", record.getService()),
					Filters.eq("host", record.getHostname()), Filters.eq("metric", record.getMetric()),
					Filters.eq("timestamp", record.getTsMonitored()));
		
		} else if (record.getType().equalsIgnoreCase("endpoint")) {
			
			return Filters.and(Filters.eq("report", this.report), Filters.eq("date_integer", record.getDateInt()),
					Filters.eq("endpoint_group", record.getGroup()), Filters.eq("service", record.getService()),
					Filters.eq("host", record.getHostname()), Filters.eq("timestamp", record.getTsMonitored()));
			
		} else if (record.getType().equalsIgnoreCase("service")) {
			
			return Filters.and(Filters.eq("report", this.report), Filters.eq("date_integer", record.getDateInt()),
					Filters.eq("endpoint_group", record.getGroup()), Filters.eq("service", record.getService()),
					Filters.eq("timestamp", record.getTsMonitored()));
		
		} else if (record.getType().equalsIgnoreCase("endpoint_group")) {
			
			return Filters.and(Filters.eq("report", this.report), Filters.eq("date_integer", record.getDateInt()),
					Filters.eq("endpoint_group", record.getGroup()), Filters.eq("timestamp", record.getTsMonitored()));

		}
		
		return null;
		
	
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
	
	public StatusEvent jsonToStatusEvent(JsonObject jRoot) {
		
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
		String monHost = extractJson("monitor_host",jRoot);
		String repeat = extractJson("repeat",jRoot);
		String message = extractJson("message",jRoot);
		String summary = extractJson("summary",jRoot);
		return new StatusEvent(rep,tp,dt,eGroup,service,hostname,metric,status,monHost,tsm,tsp,prevTs,prevStatus,repeat,summary,message);
	}

	/**
	 * Store a MongoDB document record
	 */
	@Override
	public void writeRecord(String recordJSON) throws IOException {

		JsonParser jsonParser = new JsonParser();
		// parse the json root object
		JsonObject jRoot = jsonParser.parse(recordJSON).getAsJsonObject();
		StatusEvent record = jsonToStatusEvent(jRoot);
		
		
		// Mongo Document to be prepared according to StatusType of input
		Document doc = prepDoc(record);

		if (this.method == MongoMethod.UPSERT) {

			// Filter for upsert to be prepared according to StatusType of input
			Bson f = prepFilter(record);
			UpdateOptions opts = new UpdateOptions().upsert(true);
			if (record.getType().equalsIgnoreCase("metric")) {
				metricCol.replaceOne(f, doc, opts);
			} else if (record.getType().equalsIgnoreCase("endpoint")) {
				endpointCol.replaceOne(f, doc, opts);
			} else if (record.getType().equalsIgnoreCase("service")) {
				serviceCol.replaceOne(f, doc, opts);
			}  else if (record.getType().equalsIgnoreCase("endpoint_group")) {
				egroupCol.replaceOne(f, doc, opts);
			}
			
		} else {
			if (record.getType().equalsIgnoreCase("metric")) {
				metricCol.insertOne(doc);
			} else if (record.getType().equalsIgnoreCase("endpoint")) {
				endpointCol.insertOne(doc);
			} else if (record.getType().equalsIgnoreCase("service")) {
				serviceCol.insertOne(doc);
			}  else if (record.getType().equalsIgnoreCase("endpoint_group")) {
				egroupCol.insertOne(doc);
			}
		}
	}

	/**
	 * Close MongoDB Connection
	 */
	@Override
	public void close() throws IOException {
		if (mClient != null) {
			mClient.close();
			mClient = null;
			mDB = null;
			metricCol = null;
			endpointCol = null;
			serviceCol = null;
			egroupCol = null; 
		}
	}

	@Override
	public void configure(Configuration arg0) {
		// configure

	}

}