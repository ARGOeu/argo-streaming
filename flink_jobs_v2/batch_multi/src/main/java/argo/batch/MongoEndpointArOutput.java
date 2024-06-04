package argo.batch;

import argo.ar.EndpointAR;
import com.google.gson.Gson;
import com.mongodb.ConnectionString;
import java.io.IOException;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import java.util.Iterator;
import org.bson.types.ObjectId;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.simple.JSONObject;

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
    private ObjectId nowId;
    private String uri;

    // constructor
    public MongoEndpointArOutput(String uri, String col, String method, String report, String date, boolean clearMongo) {
        this.date = Integer.parseInt(date.replace("-", ""));
        this.report = report;

        if (method.equalsIgnoreCase("upsert")) {
            this.method = MongoMethod.UPSERT;
        } else {
            this.method = MongoMethod.INSERT;
        }
        this.uri = uri;
        ConnectionString connectionString = new ConnectionString(this.uri);

        String[] hostParts = connectionString.getHosts().get(0).split(":");
        String hostname = hostParts[0];
        int port = Integer.parseInt(hostParts[1]);

        this.mongoHost = hostname;
        this.mongoPort = port;
        this.dbName = connectionString.getDatabase();
        this.colName = col;
        this.clearMongo = clearMongo;

    }

    // constructor
    public MongoEndpointArOutput(String host, int port, String db, String col, MongoMethod method, String report, String date, boolean clearMongo) {
        this.date = Integer.parseInt(date.replace("-", ""));
        this.report = report;

        this.mongoHost = host;
        this.mongoPort = port;
        this.dbName = db;
        this.colName = col;
        this.method = method;
        this.clearMongo = clearMongo;
        this.uri = "mongodb://" + this.mongoHost + ":" + this.mongoPort;
    
    }

    private void initMongo() {
        ConnectionString connectionString = new ConnectionString(this.uri);

        this.mClient = MongoClients.create(connectionString);
        this.mDB = mClient.getDatabase(dbName);
        this.mCol = mDB.getCollection(colName);
        if (this.clearMongo) {
            String oidString = Long.toHexString(new DateTime(DateTimeZone.UTC).getMillis() / 1000L) + "0000000000000000";
            this.nowId = new ObjectId(oidString);
        }

    }

    private void deleteDoc() {

        Bson filter = Filters.and(Filters.eq("report", this.report), Filters.eq("date", this.date), Filters.lte("_id", this.nowId));
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

        doc.append("info", parseInfo(info));

        if (this.method == MongoMethod.UPSERT) {
            Bson f = Filters.and(Filters.eq("report", record.getReport()), Filters.eq("date", record.getDateInt()),
                    Filters.eq("name", record.getName()), Filters.eq("service", record.getService()), Filters.eq("supergroup", record.getGroup()));

            ReplaceOptions opts = new ReplaceOptions().upsert(true);

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
        if (clearMongo) {
            deleteDoc();
        }

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

    private Document parseInfo(String info) {
        Gson g = new Gson();
        JSONObject jsonObject = g.fromJson(info, JSONObject.class);

        Iterator<String> keys = jsonObject.keySet().iterator();
        Document infoDoc = new Document();

        while (keys.hasNext()) {

            String key = keys.next();
            String value = (String) jsonObject.get(key);
            infoDoc.append(key, value);
        }
        return infoDoc;

    }

}
