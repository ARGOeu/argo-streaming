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

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

public class InfluxConnection {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxConnection.class);

    private final InfluxDBClient client;
    private final WriteApi writeApi;

    private final boolean verifySsl;

    public InfluxConnection(String url,
                            String token,
                            String org,
                            String bucket,
                            String proxy,
                            int proxyPort,
                            boolean verifySsl) {

        this.verifySsl = verifySsl;

        OkHttpClient.Builder okHttpBuilder = buildHttpClient();

        if (proxy != null && !proxy.isEmpty()) {
            Proxy proxyHost = new Proxy(
                    Proxy.Type.HTTP,
                    new InetSocketAddress(proxy, proxyPort));

            okHttpBuilder.proxy(proxyHost);
        }

        InfluxDBClientOptions options = InfluxDBClientOptions.builder()
                .url(url)
                .bucket(bucket)
                .org(org)
                .authenticateToken(token.toCharArray())
                .okHttpClient(okHttpBuilder)
                .build();

        this.client = InfluxDBClientFactory.create(options);

        if (this.client == null) {
            throw new NullPointerException("ERROR: InfluxDBClient is null");
        }

        this.writeApi = client.makeWriteApi();

        LOG.info("InfluxDB connection initialized");
    }

    /**
     * Creates the HTTP client used by the InfluxDB client.
     * By default SSL certificates are verified.
     * If verifySsl is false, all certificates and hostnames are accepted.
     */
    private OkHttpClient.Builder buildHttpClient() {

        OkHttpClient.Builder builder = new OkHttpClient.Builder();

        if (verifySsl) {
            return builder;
        }

        try {
            TrustManager[] trustAllCerts = new TrustManager[]{
                    new X509TrustManager() {
                        @Override
                        public void checkClientTrusted(X509Certificate[] chain, String authType) {
                        }

                        @Override
                        public void checkServerTrusted(X509Certificate[] chain, String authType) {
                        }

                        @Override
                        public X509Certificate[] getAcceptedIssuers() {
                            return new X509Certificate[0];
                        }
                    }
            };

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustAllCerts, new SecureRandom());

            builder.sslSocketFactory(
                    sslContext.getSocketFactory(),
                    (X509TrustManager) trustAllCerts[0]);

            builder.hostnameVerifier((hostname, session) -> true);

            LOG.warn("InfluxDB SSL certificate verification is DISABLED.");

        } catch (Exception e) {
            throw new RuntimeException("Failed to create insecure HTTP client", e);
        }

        return builder;
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