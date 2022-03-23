package argo.batch;

import java.io.IOException;
import java.util.List;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import argo.avro.MetricProfile;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.TreeMap;
import profilesmanager.AggregationProfileManager;
import profilesmanager.MetricProfileManager;
import profilesmanager.OperationsManager;

import timelines.Timeline;
import utils.Utils;

/**
 * Accepts a list o status metrics grouped by the fields: endpoint group,
 * service, endpoint Uses Continuous Timelines and Aggregators to calculate the
 * status results of a service endpoint Prepares the data in a form aligned with
 * the datastore schema for status endpoint collection
 */
public class CalcMetricTimeline extends RichGroupReduceFunction<StatusMetric, StatusTimeline> {

    private static final long serialVersionUID = 1L;

    final ParameterTool params;

    public CalcMetricTimeline(ParameterTool params) {
        this.params = params;
    }

    static Logger LOG = LoggerFactory.getLogger(CalcMetricTimeline.class);
    private List<MetricProfile> mps;
    private List<String> aps;
    private List<String> ops;
    private MetricProfileManager mpsMgr;
    private AggregationProfileManager apsMgr;
    private OperationsManager opsMgr;
    private String runDate;

    @Override
    public void open(Configuration parameters) throws IOException {
        this.runDate = params.getRequired("run.date");
        // Get data from broadcast variables
        this.mps = getRuntimeContext().getBroadcastVariable("mps");
        this.aps = getRuntimeContext().getBroadcastVariable("aps");
        this.ops = getRuntimeContext().getBroadcastVariable("ops");
        // Initialize metric profile manager
        this.mpsMgr = new MetricProfileManager();
        this.mpsMgr.loadFromList(mps);
        // Initialize aggregation profile manager
        this.apsMgr = new AggregationProfileManager();

        this.apsMgr.loadJsonString(aps);
        // Initialize operations manager
        this.opsMgr = new OperationsManager();
        this.opsMgr.loadJsonString(ops);
    }

    @Override
    public void reduce(Iterable<StatusMetric> in, Collector<StatusTimeline> out) throws Exception {
        int i = 0;
        String service = "";
        String function = "";
        String endpointGroup = "";
        String hostname = "";
        String metric = "";
        boolean hasThr = false;
        TreeMap<DateTime, Integer> timeStatusMap = new TreeMap<>();
        for (StatusMetric item : in) {
            service = item.getService();
            endpointGroup = item.getGroup();
            hostname = item.getHostname();
            function = item.getFunction();
            metric = item.getMetric();
            String ts = item.getTimestamp();
            String status = item.getStatus();
            if (i == 0) {
                int st = this.opsMgr.getIntStatus(item.getPrevState());
                timeStatusMap.put(Utils.convertStringtoDate("yyyy-MM-dd'T'HH:mm:ss'Z'", item.getPrevTs()), st);

            }
            int st = this.opsMgr.getIntStatus(status);
            timeStatusMap.put(Utils.convertStringtoDate("yyyy-MM-dd'T'HH:mm:ss'Z'", ts), st);
            if (!item.getOgStatus().equals("")) {
                hasThr = true;
            }
            i++;

        }

        Timeline timeline = new Timeline();
        timeline.insertDateTimeStamps(timeStatusMap, false);
        timeline.replacePreviousDateStatus(Utils.convertStringtoDate("yyyy-MM-dd", this.runDate), this.opsMgr.getStates(), false);//handle the first timestamp to contain the previous days timestamp status if necessary and the last timestamp to contain the status of the last timelines's entry
        ArrayList<TimeStatus> timestatusList = new ArrayList<TimeStatus>();
        for (Entry<DateTime, Integer> entry : timeline.getSamples()) {
            TimeStatus timestatus = new TimeStatus(entry.getKey().getMillis(), entry.getValue());
            timestatusList.add(timestatus);
        }

        StatusTimeline statusTimeline = new StatusTimeline(endpointGroup, function, service, hostname, metric, timestatusList);
        statusTimeline.setHasThr(hasThr);
        out.collect(statusTimeline);
    }

}
