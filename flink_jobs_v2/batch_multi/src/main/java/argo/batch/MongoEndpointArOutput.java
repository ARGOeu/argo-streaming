package argo.batch;

import argo.ar.EndpointAR;
import java.io.IOException;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;

/**
 * MongoOutputFormat for storing Service AR data to mongodb
 */
public class MongoEndpointArOutput implements OutputFormat<EndpointAR> {

	public enum MongoMethod {
		INSERT, UPSERT
	};

	private static final long serialVersionUID = 1L;

	private String mongoHost;
	private int mongoPort;
	private String dbName;
	private String colName;
	private MongoMethod method;

	private MongoClient mClient;
	private MongoDatabase mDB;
	private MongoCollection<Document> mCol;
        private boolean clearMongo;
        private String report;
        private int date;
   
 
	// constructor
	public MongoEndpointArOutput(String uri, String col, String method, String report, String date, boolean clearMongo) {

		if (method.equalsIgnoreCase("upsert")) {
			this.method = MongoMethod.UPSERT;
		} else {
			this.method = MongoMethod.INSERT;
		}

		MongoClientURI mURI = new MongoClientURI(uri);
		String[] hostParts = mURI.getHosts().get(0).split(":");
		String hostname = hostParts[0];
		int port = Integer.parseInt(hostParts[1]);

		this.mongoHost = hostname;
		this.mongoPort = port;
		this.dbName = mURI.getDatabase();
		this.colName = col;
                this.clearMongo = clearMongo;
    
	}

	// constructor
	public MongoEndpointArOutput(String host, int port, String db, String col, MongoMethod method, String report, String date, boolean clearMongo) {
		this.mongoHost = host;
		this.mongoPort = port;
		this.dbName = db;
		this.colName = col;
		this.method = method;
                this.clearMongo = clearMongo;
    	}

	private void initMongo() {
		this.mClient = new MongoClient(mongoHost, mongoPort);
		this.mDB = mClient.getDatabase(dbName);
		this.mCol = mDB.getCollection(colName);
                if (this.clearMongo) {
                  deleteDoc();
                 }
    
	}
        private void deleteDoc() {

        Bson filter = Filters.and(Filters.eq("report", this.report), Filters.eq("date", this.date));
        mCol.deleteMany(filter);
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
	 * Store a MongoDB document record
	 */
	@Override
	public void writeRecord(EndpointAR record) throws IOException {
		
		
		String info = record.getInfo();
		    
		// create document from record
		Document doc = new Document("report", record.getReport()).append("date", record.getDateInt())
				.append("name", record.getName()).append("service", record.getService()).append("supergroup", record.getGroup())
				.append("availability", record.getA()).append("reliability", record.getR()).append("up", record.getUp())
				.append("unknown", record.getUnknown()).append("down", record.getDown());
		
		if (!info.equalsIgnoreCase("")) {
			Document infoDoc = new Document();
			String[] kvs = info.split(",");
			for (String kv : kvs) {
				String[] kvtok = kv.split(":",2);
				if (kvtok.length == 2){
					infoDoc.append(kvtok[0], kvtok[1]);
				}
			}
			
			doc.append("info", infoDoc);
			
		}

		if (this.method == MongoMethod.UPSERT) {
			Bson f = Filters.and(Filters.eq("report", record.getReport()), Filters.eq("date", record.getDateInt()),
					Filters.eq("name", record.getName()), Filters.eq("service", record.getService()), Filters.eq("supergroup", record.getGroup()));

			UpdateOptions opts = new UpdateOptions().upsert(true);

			mCol.replaceOne(f, doc, opts);
		} else {
			mCol.insertOne(doc);
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
			mCol = null;
		}
	}

	@Override
	public void configure(Configuration arg0) {
		// configure

	}

}