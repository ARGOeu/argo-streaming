package trends.calculations;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import java.io.IOException;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.bson.Document;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * MongoTrendsOutput for storing trends data to mongodb
 */
public class MongoTrendsOutput implements OutputFormat<Trends> {

    // Select the type of status input
    public enum TrendsType {
        TRENDS_STATUS_METRIC, TRENDS_STATUS_ENDPOINT, TRENDS_STATUS_SERVICE, TRENDS_STATUS_GROUP, TRENDS_METRIC, TRENDS_ENDPOINT, TRENDS_SERVICE, TRENDS_GROUP
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
    private ObjectId nowId;
    private String uri;

    // constructor
    public MongoTrendsOutput(String uri, String col, TrendsType trendsType, String report, String date, boolean clearMongo) {

        this.date = Integer.parseInt(date.replace("-", ""));
        this.trendsType = trendsType;
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
    public MongoTrendsOutput(String host, int port, String db, String col, TrendsType trendsType, String report, String date, boolean clearMongo) {
        this.date = Integer.parseInt(date.replace("-", ""));
        this.report = report;

        this.mongoHost = host;
        this.mongoPort = port;
        this.dbName = db;
        this.colName = col;
        this.trendsType = trendsType;
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
                doc.append("tags", parseTags(record.getTags()));

                break;
            case TRENDS_STATUS_METRIC:

                doc.append("group", record.getGroup());
                doc.append("service", record.getService());
                doc.append("endpoint", record.getEndpoint());
                doc.append("metric", record.getMetric());
                doc.append("status", record.getStatus());
                doc.append("duration", record.getDuration());
                doc.append("trends", record.getTrends());
                doc.append("tags", parseTags(record.getTags()));
                break;
            case TRENDS_STATUS_ENDPOINT:
                doc.append("group", record.getGroup());
                doc.append("service", record.getService());
                doc.append("endpoint", record.getEndpoint());
                doc.append("status", record.getStatus());
                doc.append("duration", record.getDuration());
                doc.append("trends", record.getTrends());
                break;
            case TRENDS_STATUS_SERVICE:
                doc.append("group", record.getGroup());
                doc.append("service", record.getService());
                doc.append("status", record.getStatus());
                doc.append("duration", record.getDuration());
                doc.append("trends", record.getTrends());
                break;
            case TRENDS_STATUS_GROUP:
                doc.append("group", record.getGroup());
                doc.append("status", record.getStatus());
                doc.append("duration", record.getDuration());
                doc.append("trends", record.getTrends());
                break;
            default:
                break;
        }
        return doc;
    }

    private void deleteDoc() {

        Bson filter = Filters.and(Filters.eq("report", this.report), Filters.eq("date", this.date), Filters.lte("_id", this.nowId));
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

    private List<String> parseTags(String tags) {

        List<String> tagsList = new ArrayList<>();
        if (!tags.equals("")) {
            String[] tagsArr = tags.split(",");
            tagsList = Arrays.asList(tagsArr);
        }
        return tagsList;
    }

}