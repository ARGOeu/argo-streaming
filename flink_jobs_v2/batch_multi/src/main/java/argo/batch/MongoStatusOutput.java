package argo.batch;

import com.google.gson.Gson;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;

import java.io.IOException;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.bson.types.ObjectId;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.simple.JSONObject;

/**
 * MongoOutputFormat for storing status data to mongodb
 */
public class MongoStatusOutput implements OutputFormat<StatusMetric> {

    public enum MongoMethod {
        INSERT, UPSERT
    }

    ;

    // Select the type of status input
    public enum StatusType {
        STATUS_METRIC, STATUS_ENDPOINT, STATUS_SERVICE, STATUS_ENDPOINT_GROUP
    }

    private static final long serialVersionUID = 1L;

    private String mongoHost;
    private int mongoPort;
    private String dbName;
    private String colName;
    private MongoMethod method;
    private StatusType sType;
    private String report;

    private MongoClient mClient;
    private MongoDatabase mDB;
    private MongoCollection<Document> mCol;
    private boolean clearMongo;
    private int date;
    private ObjectId nowId;
    private String uri;

    // constructor
    public MongoStatusOutput(String uri, String col, String method, StatusType sType, String report, String date, boolean clearMongo) {
        this.date = Integer.parseInt(date.replace("-", ""));
        this.report = report;


        if (method.equalsIgnoreCase("upsert")) {
            this.method = MongoMethod.UPSERT;
        } else {
            this.method = MongoMethod.INSERT;
        }

        this.sType = sType;
        this.report = report;
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
    public MongoStatusOutput(String host, int port, String db, String col, MongoMethod method, StatusType sType,
                             String report, String date, boolean clearMongo) {

        this.date = Integer.parseInt(date.replace("-", ""));
        this.report = report;

        this.mongoHost = host;
        this.mongoPort = port;
        this.dbName = db;
        this.colName = col;
        this.method = method;
        this.sType = sType;
        this.report = report;
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

        Bson filter = Filters.and(Filters.eq("report", this.report), Filters.eq("date_integer", this.date), Filters.lte("_id", this.nowId));
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
     * Prepare correct MongoDocument according to record values and selected
     * StatusType. A different document is needed for storing Status Metric
     * results than Endpoint, Service or Endpoint Group ones.
     */
    private Document prepDoc(StatusMetric record) {

        Document doc = new Document("report", this.report)
                .append("endpoint_group", record.getGroup());

        if (this.sType == StatusType.STATUS_SERVICE) {

            doc.append("service", record.getService());

        } else if (this.sType == StatusType.STATUS_ENDPOINT) {

            doc.append("service", record.getService())
                    .append("host", record.getHostname());

            String info = record.getInfo();
            doc.append("info", parseInfo(info));

        } else if (this.sType == StatusType.STATUS_METRIC) {

            doc.append("service", record.getService())
                    .append("host", record.getHostname())
                    .append("metric", record.getMetric())
                    .append("tags", parseTags(record.getTags()))
                    .append("message", record.getMessage())
                    .append("summary", record.getSummary())
                    .append("time_integer", record.getTimeInt())
                    .append("previous_state", record.getPrevState())
                    .append("previous_timestamp", record.getPrevTs())
                    // append the actual data to status metric record in datastore
                    .append("actual_data", record.getActualData())
                    // append original status and threshold rule applied
                    .append("original_status", record.getOgStatus())
                    .append("threshold_rule_applied", record.getRuleApplied());
            String info = record.getInfo();
            doc.append("info", parseInfo(info));

        }

        doc.append("status", record.getStatus())
                .append("timestamp", record.getTimestamp())
                .append("date_integer", record.getDateInt())
                .append("has_threshold_rule", record.getHasThr());
        return doc;
    }

    /**
     * Prepare correct Update filter according to record values and selected
     * StatusType. A different update filter is needed for updating Status
     * Metric results than Endpoint, Service or Endpoint Group ones.
     */
    private Bson prepFilter(StatusMetric record) {

        if (this.sType == StatusType.STATUS_METRIC) {

            return Filters.and(Filters.eq("report", this.report), Filters.eq("date_integer", record.getDateInt()),
                    Filters.eq("endpoint_group", record.getGroup()), Filters.eq("service", record.getService()),
                    Filters.eq("host", record.getHostname()), Filters.eq("metric", record.getMetric()),
                    Filters.eq("timestamp", record.getTimestamp()));

        } else if (this.sType == StatusType.STATUS_ENDPOINT) {

            return Filters.and(Filters.eq("report", this.report), Filters.eq("date_integer", record.getDateInt()),
                    Filters.eq("endpoint_group", record.getGroup()), Filters.eq("service", record.getService()),
                    Filters.eq("host", record.getHostname()), Filters.eq("timestamp", record.getTimestamp()));

        } else if (this.sType == StatusType.STATUS_SERVICE) {

            return Filters.and(Filters.eq("report", this.report), Filters.eq("date_integer", record.getDateInt()),
                    Filters.eq("endpoint_group", record.getGroup()), Filters.eq("service", record.getService()),
                    Filters.eq("timestamp", record.getTimestamp()));

        } else {

            return Filters.and(Filters.eq("report", this.report), Filters.eq("date_integer", record.getDateInt()),
                    Filters.eq("endpoint_group", record.getGroup()), Filters.eq("timestamp", record.getTimestamp()));

        }

    }

    /**
     * Store a MongoDB document record
     */
    @Override
    public void writeRecord(StatusMetric record) throws IOException {

        // Mongo Document to be prepared according to StatusType of input
        Document doc = prepDoc(record);

        if (this.method == MongoMethod.UPSERT) {

            // Filter for upsert to be prepared according to StatusType of input
            Bson f = prepFilter(record);
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

    private List<String> parseTags(String tags) {

        List<String> tagsList = new ArrayList<>();
        if (!tags.equals("")) {
            String[] tagsArr = tags.split(",");
            tagsList = Arrays.asList(tagsArr);
        }
        return tagsList;
    }
}
