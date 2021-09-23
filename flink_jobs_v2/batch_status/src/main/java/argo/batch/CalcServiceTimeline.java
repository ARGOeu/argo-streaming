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
public class CalcServiceTimeline extends RichGroupReduceFunction<StatusTimeline, StatusTimeline> {

    private static final long serialVersionUID = 1L;

    final ParameterTool params;

    public CalcServiceTimeline(ParameterTool params) {
        this.params = params;
    }

    static Logger LOG = LoggerFactory.getLogger(ArgoStatusBatch.class);

    private List<String> aps;
    private List<String> ops;
    private AggregationProfileManager apsMgr;
    private OperationsManager opsMgr;
    private String runDate;
    private HashMap<String, String> serviceFunctionsMap;

    @Override
    public void open(Configuration parameters) throws IOException {

        this.runDate = params.getRequired("run.date");
        // Get data from broadcast variables
        this.aps = getRuntimeContext().getBroadcastVariable("aps");
        this.ops = getRuntimeContext().getBroadcastVariable("ops");
        // Initialize aggregation profile manager
        this.apsMgr = new AggregationProfileManager();

        this.apsMgr.loadJsonString(aps);
        // Initialize operations manager
        this.opsMgr = new OperationsManager();
        this.opsMgr.loadJsonString(ops);

        this.runDate = params.getRequired("run.date");
         this.serviceFunctionsMap = this.apsMgr.retrieveServiceOperations();
    }

    @Override
    public void reduce(Iterable<StatusTimeline> in, Collector<StatusTimeline> out) throws Exception {

        String service = "";
        String endpointGroup = "";
        String hostname = "";

        ArrayList<StatusMetric> statusMetrics = new ArrayList();
        HashMap<String, Timeline> timelinelist = new HashMap<>();

        for (StatusTimeline item : in) {
            service = item.getService();
            endpointGroup = item.getGroup();

            statusMetrics = item.getStatusMetrics();
            ArrayList<TimeStatus> timestatusList = item.getTimestamps();
            TreeMap<DateTime, Integer> samples = new TreeMap<>();
            for (TimeStatus timestatus : timestatusList) {
                DateTime dt = new DateTime(timestatus.getTimestamp());
                samples.put(dt, timestatus.getStatus());
            }
            Timeline timeline = new Timeline();
            timeline.insertDateTimeStamps(samples, true);

            timelinelist.put(item.getHostname(), timeline);
        }

        String operation = serviceFunctionsMap.get(service);
  
        TimelineAggregator timelineAggregator = new TimelineAggregator(timelinelist);
        timelineAggregator.aggregate(this.opsMgr.getTruthTable(), this.opsMgr.getIntOperation(operation));

        Timeline mergedTimeline = timelineAggregator.getOutput(); //collect all timelines that correspond to the group service endpoint group , merge them in order to create one timeline

        ArrayList<TimeStatus> timestatuCol = new ArrayList();
        for (Map.Entry<DateTime, Integer> entry : mergedTimeline.getSamples()) {
            TimeStatus timestatus = new TimeStatus(entry.getKey().getMillis(), entry.getValue());
            timestatuCol.add(timestatus);
        }
        
     
        StatusTimeline statusMetricTimeline = new StatusTimeline(endpointGroup, service, "", "", statusMetrics, timestatuCol);

        out.collect(statusMetricTimeline);

      
    

}

}
