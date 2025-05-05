package argo.batch;

import argo.avro.Downtime;
import argo.avro.MetricProfile;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profilesmanager.*;
import timelines.Timeline;
import timelines.TimelineAggregator;
import utils.RecompTimelineBuilder;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

/**
 * Accepts a list o status metrics grouped by the fields: endpoint group,
 * service, endpoint Uses Continuous Timelines and Aggregators to calculate the
 * status results of a service endpoint Prepares the data in a form aligned with
 * the datastore schema for status endpoint collection
 */
public class CalcEndpointTimeline extends RichGroupReduceFunction<StatusTimeline, StatusTimeline> {

    private static final long serialVersionUID = 1L;

    final ParameterTool params;

    public CalcEndpointTimeline(ParameterTool params, DateTime now) {
        this.params = params;
        this.now = now;
    }

    static Logger LOG = LoggerFactory.getLogger(ArgoMultiJob.class);

    private List<MetricProfile> mps;
    private List<String> aps;
    private List<String> ops;
    private MetricProfileManager mpsMgr;
    private AggregationProfileManager apsMgr;
    private OperationsManager opsMgr;
    private String runDate;
    private String operation;
    private DowntimeManager downtimeMgr;
    private List<Downtime> downtime;
    private DateTime now;
    private List<String> recs;
    private RecomputationsManager recMgr;

    @Override
    public void open(Configuration parameters) throws IOException, ParseException {

        this.runDate = params.getRequired("run.date");
        // Get data from broadcast variables
        this.mps = getRuntimeContext().getBroadcastVariable("mps");
        this.aps = getRuntimeContext().getBroadcastVariable("aps");
        this.ops = getRuntimeContext().getBroadcastVariable("ops");
        this.recs = getRuntimeContext().getBroadcastVariable("rec");

        // Initialize metric profile manager
        this.mpsMgr = new MetricProfileManager();
        this.mpsMgr.loadFromList(mps);
        // Initialize aggregation profile manager
        this.apsMgr = new AggregationProfileManager();

        this.apsMgr.loadJsonString(aps);
        // Initialize operations manager
        this.opsMgr = new OperationsManager();
        this.opsMgr.loadJsonString(ops);
        this.downtime = getRuntimeContext().getBroadcastVariable("down");
        this.downtimeMgr = new DowntimeManager();
        this.downtimeMgr.loadFromList(downtime);
        this.recMgr = new RecomputationsManager();
        this.recMgr.loadJsonString(recs);

        this.runDate = params.getRequired("run.date");
        this.operation = this.apsMgr.getMetricOpByProfile();

    }

    @Override
    public void reduce(Iterable<StatusTimeline> in, Collector<StatusTimeline> out) throws Exception {

        String service = "";
        String endpointGroup = "";
        String hostname = "";
        String function = "";
        String metric = "";
        HashMap<String, Timeline> timelinelist = new HashMap<>();
        ArrayList<String> recompRequestIds = new ArrayList<>();

        boolean hasThr = false;
        for (StatusTimeline item : in) {
            service = item.getService();
            endpointGroup = item.getGroup();
            hostname = item.getHostname();
            function = item.getFunction();
            metric = item.getMetric();

            ArrayList<TimeStatus> timestatusList = item.getTimestamps();
            TreeMap<DateTime, Integer> samples = new TreeMap<>();
            for (TimeStatus timestatus : timestatusList) {
                samples.put(new DateTime(timestatus.getTimestamp(), DateTimeZone.UTC), timestatus.getStatus());
            }
            Timeline timeline = new Timeline();
            timeline.insertDateTimeStamps(samples, true);

            timelinelist.put(item.getMetric(), timeline);
            if (item.hasThr()) {
                hasThr = true;
            }

        }



        TimelineAggregator timelineAggregator = new TimelineAggregator(timelinelist, this.opsMgr.getDefaultExcludedInt(), runDate);
        timelineAggregator.aggregate(this.opsMgr.getTruthTable(), this.opsMgr.getIntOperation(operation));

        Timeline mergedTimeline = timelineAggregator.getOutput(); //collect all timelines that correspond to the group service endpoint group , merge them in order to create one timeline

        ArrayList<String[]> downPeriods = this.downtimeMgr.getPeriod(hostname, service);

        if (downPeriods != null && !downPeriods.isEmpty()) {
            for (String[] downPeriod : downPeriods) {
                mergedTimeline.fillWithStatus(downPeriod[0], downPeriod[1], this.opsMgr.getDefaultDownInt(), now);
                mergedTimeline.optimize();
            }
        }
// Attempt to find if there's a recomputation status change request for the current endpoint
        ArrayList<RecomputationsManager.RecomputationElement> recompItems = recMgr.findChangedStatusItem(
                endpointGroup, service, hostname, null, RecomputationsManager.ElementType.ENDPOINT);

        if (!recompItems.isEmpty()) { // If a recomputation request is found for this metric
            //     mergedTimeline.clear();
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

        StatusTimeline statusTimeline = new StatusTimeline(endpointGroup, function, service, hostname, "", timestatuCol);
        statusTimeline.setHasThr(hasThr);

        out.collect(statusTimeline);

    }
}
