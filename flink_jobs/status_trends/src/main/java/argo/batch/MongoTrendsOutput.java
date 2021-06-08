package argo.batch;

import java.io.IOException;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;

/**
 * MongoTrendsOutput for storing trends data to mongodb
 */
public class MongoTrendsOutput implements OutputFormat<Trends> {

    // Select the type of status input
    public enum TrendsType {
        TRENDS_STATUS, TRENDS_METRIC, TRENDS_ENDPOINT, TRENDS_SERVICE, TRENDS_GROUP
    }

    private static final long serialVersionUID = 1L;

    private String mongoHost;
    private int mongoPort;
    private String dbName;
    private String colName;
    private TrendsType trendsType;
    private String report;
    private int date;
    private MongoClient mClient;
    private MongoDatabase mDB;
    private MongoCollection<Document> mCol;
    private boolean clearMongo;

    // constructor
    public MongoTrendsOutput(String uri, String col, TrendsType trendsType, String report, String date, boolean clearMongo) {

        this.date = Integer.parseInt(date.replace("-", ""));
        this.trendsType = trendsType;
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
    public MongoTrendsOutput(String host, int port, String db, String col, TrendsType trendsType, String report, boolean clearMongo) {
        this.mongoHost = host;
        this.mongoPort = port;
        this.dbName = db;
        this.colName = col;
        this.trendsType = trendsType;
        this.report = report;
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
     * StatusType. A different document is needed for storing Trends results
     * than Metric , Endpoint, Service , Group
     */
    private Document prepDoc(Trends record) {
        Document doc = new Document("report", this.report)
                .append("date", this.date);

        switch (this.trendsType) {
            case TRENDS_GROUP:
                doc.append("group", record.getGroup());
                doc.append("flipflop", record.getFlipflop());
                break;
            case TRENDS_SERVICE:
                doc.append("group", record.getGroup());
                doc.append("service", record.getService());
                doc.append("flipflop", record.getFlipflop());
                break;
            case TRENDS_ENDPOINT:
                doc.append("group", record.getGroup());
                doc.append("service", record.getService());
                doc.append("endpoint", record.getEndpoint());
                doc.append("flipflop", record.getFlipflop());
                break;
            case TRENDS_METRIC:
                doc.append("group", record.getGroup());
                doc.append("service", record.getService());
                doc.append("endpoint", record.getEndpoint());
                doc.append("metric", record.getMetric());
                doc.append("flipflop", record.getFlipflop());
                break;
            case TRENDS_STATUS:
                doc.append("group", record.getGroup());
                doc.append("service", record.getService());
                doc.append("endpoint", record.getEndpoint());
                doc.append("metric", record.getMetric());
                doc.append("status", record.getStatus());
                doc.append("trends", record.getTrends());
                break;
            default:
                break;
        }
        return doc;
    }

    private void deleteDoc() {

        Bson filter = Filters.and(Filters.eq("report", this.report), Filters.eq("date", this.date));
        mCol.deleteMany(filter);
    }

    /**
     * Store a MongoDB document record
     */
    @Override
    public void writeRecord(Trends record) throws IOException {

        // Mongo Document to be prepared according to StatusType of input
        Document doc = prepDoc(record);

        mCol.insertOne(doc);

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
