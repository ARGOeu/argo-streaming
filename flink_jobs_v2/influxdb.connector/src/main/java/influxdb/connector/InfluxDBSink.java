package influxdb.connector;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.write.Point;
import java.io.Serializable;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
InfluxDBSink implements a sink to write streams into influx db. 
To connect the following parameters are necessary

* --influx.endpoint, the endpoint to connect to influx db
* --influx.port, the port to connecto to influx db
* --influx.token, the token to authorize access to influx db
* --influx.org, the influx organisation to connect
* --influx.bucket, the influx organisation bucket to write stream data
* --influx.proxy(OPTIONAL), the proxy hostname to connect. If no proxy is needed the parameter can be undefined
* --influx.proxyport(OPTIONAL) the proxy port to connect. If no proxy is needed the parameter can be undefined 
 */
public class InfluxDBSink extends RichSinkFunction<Point> implements Serializable {

    private static final long serialVersionUID = 1L;

    // setup logger
    static Logger LOG = LoggerFactory.getLogger(InfluxDBSink.class);

    private String url = null;
    private String endpoint = null;
    private String port = null;
    private String token = null;
    private String bucket = null;
    private String org = null;
    private transient int batch = 1;
    private transient long interval = 100L;
    private transient boolean verify = true;
    private transient boolean useProxy = false;
    private transient String proxyURL = "";
    private transient int proxyPORT;
    private transient String date;
    private transient InfluxDBClient client;
    private transient InfluxConnection connection;
    private ParameterTool params;

    public InfluxDBSink(ParameterTool params) {

        this.params = params;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        endpoint = this.params.get("influx.endpoint");
        port = this.params.get("influx.port");
        token = this.params.get("influx.token");
        org = this.params.get("influx.org");
        bucket = this.params.get("influx.bucket");
        proxyURL = this.params.get("influx.proxy");
        if (this.params.has("influx.proxyport")) {
            proxyPORT = this.params.getInt("influx.proxyport");
        }
        url = endpoint + ":" + port;

        try {
            connection = new InfluxConnection();
        } catch (NullPointerException e) {
            LOG.error("influx db connection does not exist");
        }

        client = connection.buildConnection(url, token, org, bucket, proxyURL, proxyPORT);
    }

    @Override
    public void invoke(Point in) throws Exception {
        this.connection.singlePointWrite(client, in);
        //  this.connection.deleteRecord(client);

    }

    @Override
    public void close() throws Exception {

        this.client.close();

    }

    public static Logger getLOG() {
        return LOG;
    }

    public static void setLOG(Logger LOG) {
        InfluxDBSink.LOG = LOG;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getOrg() {
        return org;
    }

    public void setOrg(String org) {
        this.org = org;
    }

    public int getBatch() {
        return batch;
    }

    public void setBatch(int batch) {
        this.batch = batch;
    }

    public long getInterval() {
        return interval;
    }

    public void setInterval(long interval) {
        this.interval = interval;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public InfluxDBClient getClient() {
        return client;
    }

    public void setClient(InfluxDBClient client) {
        this.client = client;
    }

    public InfluxConnection getConnection() {
        return connection;
    }

    public void setConnection(InfluxConnection connection) {
        this.connection = connection;
    }

}
