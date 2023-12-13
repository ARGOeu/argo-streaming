package influxdb.connector;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.write.Point;
import com.influxdb.exceptions.InfluxException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.UnknownHostException;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cthermolia
 */
public class InfluxConnection {

    static Logger LOG = LoggerFactory.getLogger(InfluxConnection.class);

    public InfluxConnection() {
    }

    public InfluxDBClient buildConnection(String url, String token, String org, String bucket, String proxy, int proxyport) throws UnknownHostException {
        InfluxDBClient cl = null;
        //try {
        InfluxDBClientOptions options = null;
        if (proxy != null) {
         
            Proxy proxyHost = new Proxy(Proxy.Type.HTTP,new InetSocketAddress(proxy, proxyport));
            OkHttpClient.Builder okHttpBuilder = new OkHttpClient.Builder()
                    .proxy(proxyHost);

            if (okHttpBuilder == null) {
                throw new NullPointerException();
            }

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
        cl = InfluxDBClientFactory.create(options);
        
       

        if (cl == null) {
            throw new NullPointerException("ERROR: InfluxDBClient is null ");
        }
        return cl;
    }

    public boolean singlePointWrite(InfluxDBClient client, Point point) {
        boolean flag = false;
       try {
        WriteApiBlocking writeApi = client.getWriteApiBlocking();
        writeApi.writePoint(point);
        flag = true;
        } catch (InfluxException e) {
            LOG.error("ERROR WRITE TO INFLUX" + e.getMessage());
        }
        return flag;
    }

}
