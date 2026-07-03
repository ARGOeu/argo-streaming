package influxdb.connector;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.write.Point;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

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
    private  boolean influx_verify = true;

    public InfluxDBSink(ParameterTool params) {

        this.params = params;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        String endpoint = params.get("influx.endpoint");
        String port = params.get("influx.port");
        token = params.get("influx.token");
        org = params.get("influx.org");
        bucket = params.get("influx.bucket");

        proxyURL = params.get("influx.proxy", null);
        proxyPORT = params.getInt("influx.proxyport", 0);

        url = endpoint + ":" + port;
        if(params.has("influx.verify")) {
            influx_verify = params.getBoolean("influx.verify");
            System.out.println("VERIFY--- "+influx_verify);
        }
        LOG.info("Opening InfluxDB sink for {}", url);
        connection = new InfluxConnection(url, token, org, bucket, proxyURL, proxyPORT,influx_verify);
    }

    @Override
    public void close(){

        this.client.close();

    }

    public String getOrg() {
        return org;
    }

    public void setOrg(String org) {
        this.org = org;
    }

    @Override
    public void invoke(Point point) {
        connection.write(point);

    }
}
