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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.joda.time.DateTimeZone;
import profilesmanager.AggregationProfileManager;
import profilesmanager.OperationsManager;

import timelines.Timeline;
import timelines.TimelineAggregator;

/**
 * Accepts a list o status metrics grouped by the fields: endpoint group,
 * service, endpoint Uses Continuous Timelines and Aggregators to calculate the
 * status results of a service endpoint Prepares the data in a form aligned with
 * the datastore schema for status endpoint collection
 */
public class CalcGroupFunctionTimeline extends RichGroupReduceFunction<StatusTimeline, StatusTimeline> {

    private static final long serialVersionUID = 1L;

    final ParameterTool params;

    public CalcGroupFunctionTimeline(ParameterTool params) {
        this.params = params;
    }
    static Logger LOG = LoggerFactory.getLogger(ArgoMultiJob.class);
    private List<String> aps;
    private List<String> ops;
    private AggregationProfileManager apsMgr;
    private OperationsManager opsMgr;
    private String runDate;
    private HashMap<String, String> functionOperations;

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

        this.functionOperations = this.apsMgr.retrieveGroupOperations();

    }

    @Override
    public void reduce(Iterable<StatusTimeline> in, Collector<StatusTimeline> out) throws Exception {

        String endpointGroup = "";
        String function = "";
        HashMap<String, Timeline> timelinelist = new HashMap<>();
        boolean hasThr = false;
        for (StatusTimeline item : in) {
            function = item.getFunction();
            endpointGroup = item.getGroup();

            ArrayList<TimeStatus> timestatusList = item.getTimestamps();
            TreeMap<DateTime, Integer> samples = new TreeMap<>();
            for (TimeStatus timestatus : timestatusList) {
                DateTime dt = new DateTime(timestatus.getTimestamp(), DateTimeZone.UTC);
                samples.put(dt, timestatus.getStatus());
            }
            Timeline timeline = new Timeline();
            timeline.insertDateTimeStamps(samples, true);

            timelinelist.put(item.getService(), timeline);
            if (item.hasThr()) {
                hasThr = true;
            }
        }

        String operation = functionOperations.get(function);  //for each function an operation exists , so retrieve the corresponding truth table
        TimelineAggregator timelineAggregator = new TimelineAggregator(timelinelist,this.opsMgr.getDefaultExcludedInt(), runDate);
        timelineAggregator.aggregate(this.opsMgr.getTruthTable(), this.opsMgr.getIntOperation(operation));

        Timeline mergedTimeline = timelineAggregator.getOutput(); //collect all timelines that correspond to the group service endpoint group , merge them in order to create one timeline

        ArrayList<TimeStatus> timestatuCol = new ArrayList();
        for (Map.Entry<DateTime, Integer> entry : mergedTimeline.getSamples()) {
            TimeStatus timestatus = new TimeStatus(entry.getKey().getMillis(), entry.getValue());
            timestatuCol.add(timestatus);
        }

        StatusTimeline statusTimeline = new StatusTimeline(endpointGroup, function, "", "", "", timestatuCol);
        statusTimeline.setHasThr(hasThr);
        out.collect(statusTimeline);
    }

}
