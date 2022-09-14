package argo.streaming;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArgoMessagingSink extends RichSinkFunction<String> {

    private static final long serialVersionUID = 1L;

    // setup logger
    static Logger LOG = LoggerFactory.getLogger(ArgoMessagingSink.class);

    private String endpoint = null;
    private String port = null;
    private String token = null;
    private String project = null;
    private String topic = null;
    private int batch = 1;
    private long interval = 100L;
    private boolean verify = true;
    private boolean useProxy = false;
    private String proxyURL = "";
    private String date;
//    private transient Object rateLck; // lock for waiting to establish rate

    //  private volatile boolean isRunning = true;
    private ArgoMessagingClient client = null;

    public ArgoMessagingSink(String endpoint, String port, String token, String project, String topic, Long interval, String date) {
        this.endpoint = endpoint;
        this.port = port;
        this.token = token;
        this.project = project;
        this.topic = topic;
        this.interval = interval;
        this.verify = true;
        this.date=date;

    }

    @Override
    public void open(Configuration parameters) throws Exception {

        String fendpoint = this.endpoint;
        if (this.port != null && !this.port.isEmpty()) {
            fendpoint = this.endpoint + ":" + port;
        }
        client = new ArgoMessagingClient("https", this.token, fendpoint, this.project, this.topic, this.verify, this.date);
        if (this.useProxy) {
            client.setProxy(this.proxyURL);
        }
    }

    @Override
    public void invoke(String in) throws Exception {
        //  boolean isValid = true;
        // boolean isValid=this.client.validate();
        //if (isValid) {
        this.client.publish(in);
        //}
    }

    @Override
    public void close() throws Exception {

        this.client.close();

    }

    /**
     * Set verify to true or false. If set to false AMS client will be able to
     * contact AMS endpoints that use self-signed certificates
     */
    public void setVerify(boolean verify) {
        this.verify = verify;
    }

    /**
     * Set proxy details for AMS client
     */
    public void setProxy(String proxyURL) {
        this.useProxy = true;
        this.proxyURL = proxyURL;
    }

    /**
     * Unset proxy details for AMS client
     */
    public void unsetProxy(String proxyURL) {
        this.useProxy = false;
        this.proxyURL = "";
    }

}
