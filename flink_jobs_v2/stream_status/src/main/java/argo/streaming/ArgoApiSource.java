package argo.streaming;

import Utils.IntervalType;
import argo.amr.ApiResource;
import argo.amr.ApiResourceManager;

import java.time.Duration;
import java.time.Instant;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom source to connect to ArgoWebApi service. Uses API Resource Manager
 */
public class ArgoApiSource extends RichSourceFunction<Tuple2<String, String>> {

    private static final long serialVersionUID = 1L;

    // setup logger
    static Logger LOG = LoggerFactory.getLogger(ArgoApiSource.class);

    private String endpoint = null;
    private String token = null;
    private String reportID = null;
    private int syncInterval = 24;
    private IntervalType intervalType = IntervalType.HOURS;

    private long interval = 100L;
    private boolean verify = true;
    private boolean useProxy = false;
    private String proxyURL = "";
    private transient Object rateLck; // lock for waiting to establish rate

    private volatile boolean isRunning = true;

    private ApiResourceManager client = null;
    private Instant timeSnapshot = null;
    private int checkApiInterval = 1;
    private IntervalType checkApiIntervalType = IntervalType.HOURS;
    private boolean shouldCheckApi = false;

    public ArgoApiSource(String endpoint, String token, String reportID, String syncInterval, Long interval, String checkApiInterval) {
        this.endpoint = endpoint;
        this.token = token;
        this.reportID = reportID;
        this.interval = interval;
        this.verify = true;
        if (syncInterval != null) {
            setSyncUpdate(syncInterval);
        }

        if (checkApiInterval != null) {
            setCheckApiInterval(checkApiInterval);
        }
    }

    private void setSyncUpdate(String syncInterval) {
        AmsStreamStatus.IntervalStruct intervalStruct = AmsStreamStatus.parseInterval(syncInterval);

        if (intervalStruct.getIntervalType() == null) {
            this.syncInterval = 24;
            this.intervalType = IntervalType.HOURS;
        } else {
            this.syncInterval = intervalStruct.intervalValue;
            this.intervalType = intervalStruct.intervalType;
        }

    }

    private void setCheckApiInterval(String checkApiInterval) {
        AmsStreamStatus.IntervalStruct intervalStruct = AmsStreamStatus.parseInterval(checkApiInterval);

        if (intervalStruct.getIntervalType() == null) {
            this.checkApiInterval = 24;
            this.checkApiIntervalType = IntervalType.HOURS;
        } else {
            this.checkApiInterval = intervalStruct.intervalValue;
            this.checkApiIntervalType = intervalStruct.intervalType;
        }

    }

    /**
     * Set verify to true or false. If set to false AMS client will be able to contact AMS endpoints that use self-signed certificates
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


    @Override
    public void cancel() {
        isRunning = false;

    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        // This is the main run logic
        while (isRunning) {

            // check if interval in hours has passed to make a move
            Instant ti = Instant.now();
            Duration td = Duration.between(this.timeSnapshot, ti);
            // interval has passed do consume from api
            if (isTimeForSync(td) || shouldCheckApi(td)) {
                this.timeSnapshot = ti;
                // retrieve info from api
                try {
                    this.client.getRemoteAll();

                    Tuple2<String, String> mt = new Tuple2<String, String>("metric_profile", client.getResourceJSON(ApiResource.METRIC));
                    Tuple2<String, String> gt = new Tuple2<String, String>("group_endpoints", client.getResourceJSON(ApiResource.TOPOENDPOINTS));
                    Tuple2<String, String> dt = new Tuple2<String, String>("downtimes", client.getResourceJSON(ApiResource.DOWNTIMES));

                    ctx.collect(mt);
                    ctx.collect(gt);
                    ctx.collect(dt);
                    shouldCheckApi=false;
                } catch (Exception e) {
                    shouldCheckApi = true;
                    Log.error("Exception in ArgoApiSource due to web api error connection : ", e.getMessage());

                }
            }
            synchronized (rateLck) {
                rateLck.wait(this.interval);
            }

        }

    }

    /**
     * Argo-web-api Source initialization
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // init rate lock
        rateLck = new Object();


        this.timeSnapshot = Instant.now();

        this.client = new ApiResourceManager(this.endpoint, this.token);
        client.setReportID(this.reportID);
        client.setVerify(this.verify);
        if (this.useProxy) {
            client.setProxy(this.proxyURL);
        }

    }

    @Override
    public void close() throws Exception {
        if (this.client != null) {
            this.client = null;
        }
        synchronized (rateLck) {
            rateLck.notify();
        }
    }

    private boolean isTimeForSync(Duration td) {
        switch (this.intervalType) {
            case DAY:
                return td.toDays() > this.syncInterval;
            case HOURS:

                return td.toHours() > this.syncInterval;
            case MINUTES:

                return td.toMinutes() > this.syncInterval;
            default:
                return td.toHours() > this.syncInterval;
        }
    }

    private boolean shouldCheckApi(Duration td) {
        if(!shouldCheckApi){
            return false;
        }
        if (this.checkApiIntervalType == null) {
            // Fallback or exception for invalid enum state
            throw new IllegalStateException("checkApiIntervalType must not be null");
        }

        switch (checkApiIntervalType) {
            case DAY:
                return td.toDays() > this.checkApiInterval;
            case HOURS:
                return td.toHours() > this.checkApiInterval;
            case MINUTES:
                return td.toMinutes() > this.checkApiInterval;
            default:
                throw new UnsupportedOperationException("Unsupported interval type: " + checkApiIntervalType);
        }
    }
}