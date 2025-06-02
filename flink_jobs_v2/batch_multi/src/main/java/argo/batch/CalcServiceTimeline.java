package argo.batch;

import java.io.IOException;
import java.text.ParseException;
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

import profilesmanager.RecomputationsManager;
import timelines.Timeline;
import timelines.TimelineAggregator;
import utils.RecompTimelineBuilder;
import utils.Utils;

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

    static Logger LOG = LoggerFactory.getLogger(ArgoMultiJob.class);

    private List<String> aps;
    private List<String> ops;
    private AggregationProfileManager apsMgr;
    private OperationsManager opsMgr;
    private String runDate;
    private HashMap<String, String> serviceFunctionsMap;
    private List<HashMap<String, List<RecomputationsManager.RecomputationElement>>> rec;

    @Override
    public void open(Configuration parameters) throws IOException, ParseException {

        this.runDate = params.getRequired("run.date");
        // Get data from broadcast variables
        this.aps = getRuntimeContext().getBroadcastVariable("aps");
        this.ops = getRuntimeContext().getBroadcastVariable("ops");
        this.rec = getRuntimeContext().getBroadcastVariable("rec");
        RecomputationsManager.serviceRecomputationItems=this.rec.get(0);
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

        String hostname = "";
        String service = "";
        String endpointGroup = "";
        String function = "";
        HashMap<String, Timeline> timelinelist = new HashMap<>();
        boolean hasThr = false;
        ArrayList<String> recompRequestIds = new ArrayList<>();

        for (StatusTimeline item : in) {
            service = item.getService();
            endpointGroup = item.getGroup();
            function = item.getFunction();
            hostname = item.getHostname();

            ArrayList<TimeStatus> timestatusList = item.getTimestamps();
            TreeMap<DateTime, Integer> samples = new TreeMap<DateTime, Integer>();
            for (TimeStatus timestatus : timestatusList) {
                DateTime dt = new DateTime(timestatus.getTimestamp(), DateTimeZone.UTC);
                samples.put(dt, timestatus.getStatus());
            }

            Timeline timeline = new Timeline();
            timeline.insertDateTimeStamps(samples, true);
            timelinelist.put(item.getHostname(), timeline);
            if (item.hasThr()) {
                hasThr = true;
            }

        }

        String operation = serviceFunctionsMap.get(service);
        if (operation == null) {
            throw new RuntimeException("Operation for group:" + endpointGroup + " service: " + service + " does not exist. Check Aggregation Profiles");

        }

        TimelineAggregator timelineAggregator = new TimelineAggregator(timelinelist, this.opsMgr.getDefaultExcludedInt(), runDate);
        timelineAggregator.aggregate(this.opsMgr.getTruthTable(), this.opsMgr.getIntOperation(operation));

        Timeline mergedTimeline = timelineAggregator.getOutput(); //collect all timelines that correspond to the group service endpoint group , merge them in order to create one timeline

        ArrayList<RecomputationsManager.RecomputationElement> recompItems = RecomputationsManager.findChangedStatusItem(endpointGroup, service, null, null, RecomputationsManager.ElementType.SERVICE);

        if (!recompItems.isEmpty()) { // If a recomputation request is found for this metric
            for (RecomputationsManager.RecomputationElement recompItem : recompItems) {
                TreeMap<DateTime, Integer> recomputedSamples = (TreeMap<DateTime, Integer>) mergedTimeline.getSamplesMap().clone();
                mergedTimeline.bulkInsert(RecompTimelineBuilder.calcRecomputations(
                        recomputedSamples, // existing timeline samples
                        recompItem,                     // recomputation details
                        runDate,                        // the current day of execution
                        this.opsMgr                     // operations manager to interpret statuses
                ).entrySet());
            }
        }
        ArrayList<TimeStatus> timestatuCol = new ArrayList();
        for (Map.Entry<DateTime, Integer> entry : mergedTimeline.getSamples()) {
            TimeStatus timestatus = new TimeStatus(entry.getKey().getMillis(), entry.getValue());
            timestatuCol.add(timestatus);
        }

        StatusTimeline statusTimeline = new StatusTimeline(endpointGroup, function, service, "", "", timestatuCol);
        statusTimeline.setHasThr(hasThr);
        out.collect(statusTimeline);

    }
}
