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
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import profilesmanager.AggregationProfileManager;
import profilesmanager.MetricProfileManager;
import profilesmanager.OperationsManager;

import timelines.Timeline;
import timelines.TimelineAggregator;

/**
 * Accepts a list o status metrics grouped by the fields: endpoint group,
 * service, endpoint Uses Continuous Timelines and Aggregators to calculate the
 * status results of a service endpoint Prepares the data in a form aligned with
 * the datastore schema for status endpoint collection
 */
public class CalcEndpointTimeline extends RichGroupReduceFunction<StatusTimeline, StatusTimeline> {

    private static final long serialVersionUID = 1L;

    final ParameterTool params;

    public CalcEndpointTimeline(ParameterTool params) {
        this.params = params;
    }

    static Logger LOG = LoggerFactory.getLogger(ArgoStatusBatch.class);

    private List<MetricProfile> mps;
    private List<String> aps;
    private List<String> ops;
    private MetricProfileManager mpsMgr;
    private AggregationProfileManager apsMgr;
    private OperationsManager opsMgr;
    private String runDate;
    private String operation;

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

        this.runDate = params.getRequired("run.date");
        this.operation = this.apsMgr.getMetricOpByProfile();

    }

    @Override
    public void reduce(Iterable<StatusTimeline> in, Collector<StatusTimeline> out) throws Exception {

        String service = "";
        String endpointGroup = "";
        String hostname = "";

        TreeMap<DateTime, Integer> timeStatusMap = new TreeMap<>();
        ArrayList<StatusMetric> statusMetrics = new ArrayList();
        HashMap<String, Timeline> timelinelist = new HashMap<>();

        for (StatusTimeline item : in) {
            service = item.getService();
            endpointGroup = item.getGroup();
            hostname = item.getHostname();

            statusMetrics = item.getStatusMetrics();
            ArrayList<TimeStatus> timestatusList=item.getTimestamps();
            TreeMap<DateTime, Integer> samples=new TreeMap<>();
            for(TimeStatus timestatus: timestatusList){
                
                samples.put(new DateTime(timestatus.getTimestamp()),timestatus.getStatus());
            }
            Timeline timeline =new Timeline();
            timeline.insertDateTimeStamps(samples, true);

            timelinelist.put(item.getMetric(), timeline);
        }

        TimelineAggregator timelineAggregator = new TimelineAggregator(timelinelist);
        timelineAggregator.aggregate(this.opsMgr.getTruthTable(), this.opsMgr.getIntOperation(operation));

        Timeline mergedTimeline = timelineAggregator.getOutput(); //collect all timelines that correspond to the group service endpoint group , merge them in order to create one timeline

         ArrayList<TimeStatus> timestatuCol=new ArrayList();
        for(Map.Entry<DateTime,Integer> entry: mergedTimeline.getSamples()){
            TimeStatus timestatus=new TimeStatus(entry.getKey().getMillis(),entry.getValue());
            timestatuCol.add(timestatus);
        }
        
        StatusTimeline statusMetricTimeline = new StatusTimeline(endpointGroup, service, hostname, "", statusMetrics, timestatuCol);

        out.collect(statusMetricTimeline);

    }

}
