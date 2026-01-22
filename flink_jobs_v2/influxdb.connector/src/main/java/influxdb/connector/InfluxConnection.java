package influxdb.connector;


import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.WriteApi;
import com.influxdb.client.write.Point;
import com.influxdb.exceptions.InfluxException;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.Proxy;
public class InfluxConnection {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxConnection.class);

    private final InfluxDBClient client;
    private final WriteApi writeApi;

    public InfluxConnection(String url, String token, String org, String bucket,String proxy,int proxyPort) {

        InfluxDBClientOptions options;

        if (proxy != null && !proxy.isEmpty()) {
            Proxy proxyHost = new Proxy( Proxy.Type.HTTP, new InetSocketAddress(proxy, proxyPort));

            OkHttpClient.Builder okHttpBuilder = new OkHttpClient.Builder().proxy(proxyHost);

            options = InfluxDBClientOptions.builder()
                    .url(url)
                    .bucket(bucket)
                    .org(org)
                    .authenticateToken(token.toCharArray())
                    .okHttpClient(okHttpBuilder)
                    .build();
        } else {
            options = InfluxDBClientOptions.builder()
                    .url(url)
                    .bucket(bucket)
                    .org(org)
                    .authenticateToken(token.toCharArray())
                    .build();
        }

        this.client = InfluxDBClientFactory.create(options);

        if (this.client == null) {
            throw new NullPointerException("ERROR: InfluxDBClient is null");
        }

        // Create WriteApi ONCE
        this.writeApi = client.makeWriteApi();

        LOG.info("InfluxDB connection initialized");
    }

    /**
     * Thread-safe, async write
     */
    public void write(Point point) {
        try {
            writeApi.writePoint(point);
           } catch (InfluxException e) {
            LOG.error("ERROR WRITE TO INFLUX", e);
        }
    }

    /**
     * Flush buffered data
     */
    public void flush() {
        writeApi.flush();
    }

    /**
     * Proper shutdown
     */
    public void close() {
        try {
            LOG.info("Closing InfluxDB WriteApi");
            writeApi.close();
        } catch (Exception e) {
            LOG.warn("Error closing WriteApi", e);
        }

        try {
            LOG.info("Closing InfluxDB client");
            client.close();
        } catch (Exception e) {
            LOG.warn("Error closing InfluxDB client", e);
        }
    }
}
