package argo.batch;

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
import java.util.Arrays;
import org.bson.types.ObjectId;
import org.joda.time.DateTime;

/**
 * MongoOutputFormat for storing status data to mongodb
 */
public class MongoStatusOutput implements OutputFormat<StatusMetric> {

    public enum MongoMethod {
        INSERT, UPSERT
    };

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

    }

    private void initMongo() {
        this.mClient = new MongoClient(mongoHost, mongoPort);
        this.mDB = mClient.getDatabase(dbName);
        this.mCol = mDB.getCollection(colName);
        if (this.clearMongo) {
            String oidString = Long.toHexString(new DateTime().getMillis() / 1000L) + "0000000000000000";
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
            if (!info.equalsIgnoreCase("")) {
                Document infoDoc = new Document();
                String[] kvs = info.split(",");
                for (String kv : kvs) {
                    String[] kvtok = kv.split(":", 2);
                    if (kvtok.length == 2) {
                        infoDoc.append(kvtok[0], kvtok[1]);
                    }
                }

                doc.append("info", infoDoc);
            }

        } else if (this.sType == StatusType.STATUS_METRIC) {
            String[] tagsArr = record.getTags().split(",");
            doc.append("service", record.getService())
                    .append("host", record.getHostname())
                    .append("metric", record.getMetric())
                    .append("tags", Arrays.asList(tagsArr))
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

}
