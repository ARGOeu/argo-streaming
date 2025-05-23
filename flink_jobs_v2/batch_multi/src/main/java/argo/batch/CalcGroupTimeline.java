package argo.batch;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class CalcGroupTimeline extends RichGroupReduceFunction<StatusTimeline, StatusTimeline> {

    private static final long serialVersionUID = 1L;

    final ParameterTool params;

    public CalcGroupTimeline(ParameterTool params) {
        this.params = params;
    }

    static Logger LOG = LoggerFactory.getLogger(ArgoMultiJob.class);

    private List<String> aps;
    private List<String> ops;
    private List<String> recs;

    private AggregationProfileManager apsMgr;
    private OperationsManager opsMgr;
    private RecomputationsManager recMgr;

    private String runDate;

    @Override
    public void open(Configuration parameters) throws IOException, ParseException {

        this.runDate = params.getRequired("run.date");
        // Get data from broadcast variables
        this.aps = getRuntimeContext().getBroadcastVariable("aps");
        this.ops = getRuntimeContext().getBroadcastVariable("ops");
        this.recs = getRuntimeContext().getBroadcastVariable("rec");
        // Initialize aggregation profile manager
        this.apsMgr = new AggregationProfileManager();

        this.apsMgr.loadJsonString(aps);
        // Initialize operations manager
        this.opsMgr = new OperationsManager();
        this.opsMgr.loadJsonString(ops);
        this.recMgr = new RecomputationsManager();

        this.recMgr.loadJsonString(recs);

        this.runDate = params.getRequired("run.date");

    }

    @Override
    public void reduce(Iterable<StatusTimeline> in, Collector<StatusTimeline> out) throws Exception {
        String endpointGroup = "";
        String function = "";

        HashMap<String, Timeline> timelinelist = new HashMap<>();
        boolean hasThr = false;
        ArrayList<String> recompRequestIds = new ArrayList<>();

        for (StatusTimeline item : in) {
            endpointGroup = item.getGroup();
            function = item.getFunction();

            ArrayList<TimeStatus> timestatusList = item.getTimestamps();
            TreeMap<DateTime, Integer> samples = new TreeMap<>();
            for (TimeStatus timestatus : timestatusList) {
                DateTime dt = new DateTime(timestatus.getTimestamp(), DateTimeZone.UTC);
                samples.put(dt, timestatus.getStatus());
            }
            Timeline timeline = new Timeline();
            timeline.insertDateTimeStamps(samples, true);

            timelinelist.put(item.getFunction(), timeline);
            if (item.hasThr()) {
                hasThr = true;
            }

          }

        String groupOperation = this.apsMgr.retrieveProfileOperation();
        TimelineAggregator timelineAggregator = new TimelineAggregator(timelinelist, this.opsMgr.getDefaultExcludedInt(), runDate);
        timelineAggregator.aggregate(this.opsMgr.getTruthTable(), this.opsMgr.getIntOperation(groupOperation));

        Timeline mergedTimeline = timelineAggregator.getOutput(); //collect all timelines that correspond to the group service endpoint group , merge them in order to create one timeline

        // Find a recomputation request for the given endpoint group.
        // The search is specific to the group (ElementType.GROUP) to find changes in the status.
        ArrayList<RecomputationsManager.RecomputationElement> recompItems = recMgr.findChangedStatusItem(
                endpointGroup,   // The endpoint group for which the recomputation is being checked
                null,             // No specific service filter
                null,             // No specific hostname filter
                null,             // No specific metric filter
                RecomputationsManager.ElementType.GROUP // Element type set to GROUP for group-level recomputation
        );

        if (!recompItems.isEmpty()) { // If a recomputation request is found for this metric
            for (RecomputationsManager.RecomputationElement recompItem : recompItems) {
                TreeMap<DateTime,Integer> recomputedSamples=(TreeMap<DateTime,Integer>)mergedTimeline.getSamplesMap().clone();
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

        StatusTimeline statusTimeline = new StatusTimeline(endpointGroup, "", "", "", "", timestatuCol);
        statusTimeline.setHasThr(hasThr);
        out.collect(statusTimeline);
    }
}
