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
        this.recs = getRuntimeContext().getBroadcastVariable("recs");
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

        HashMap<String, Timeline> timelinelist = new HashMap<>();
        boolean hasThr = false;

        boolean overrideByRecomp=Boolean.FALSE;
        for (StatusTimeline item : in) {
            endpointGroup = item.getGroup();

            ArrayList<TimeStatus> timestatusList = item.getTimestamps();
            TreeMap<DateTime, Integer> samples = new TreeMap<>();

            Timeline timeline = calcRecomputation(endpointGroup, timestatusList, samples);
            overrideByRecomp=timeline.isOverrideByRecomp();
            timelinelist.put(item.getFunction(), timeline);
            if (item.hasThr()) {
                hasThr = true;
            }
        }

        String groupOperation = this.apsMgr.retrieveProfileOperation();
        TimelineAggregator timelineAggregator = new TimelineAggregator(timelinelist, this.opsMgr.getDefaultExcludedInt(), runDate);
        timelineAggregator.aggregate(this.opsMgr.getTruthTable(), this.opsMgr.getIntOperation(groupOperation));

        Timeline mergedTimeline = timelineAggregator.getOutput(); //collect all timelines that correspond to the group service endpoint group , merge them in order to create one timeline

        ArrayList<TimeStatus> timestatuCol = new ArrayList();
        for (Map.Entry<DateTime, Integer> entry : mergedTimeline.getSamples()) {
            TimeStatus timestatus = new TimeStatus(entry.getKey().getMillis(), entry.getValue());
            timestatuCol.add(timestatus);
        }

        StatusTimeline statusTimeline = new StatusTimeline(endpointGroup, "", "", "", "", timestatuCol,overrideByRecomp);
        statusTimeline.setHasThr(hasThr);
        out.collect(statusTimeline);
    }

    private Timeline calcRecomputation(String group, ArrayList<TimeStatus> timeStatusList, TreeMap<DateTime, Integer> samples) throws ParseException {
        boolean overrideByRecomp = Boolean.FALSE;
        System.out.println("timeline list-- " + timeStatusList.size() + " group " + group);
        if (group.equals("static-site")) {
            System.out.println("here it is ");

        }
        System.out.println("group is : " + group);
        String formatter = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        RecomputationsManager.ChangedStatusItem changedStatusItem = recMgr.findChangedStatusItem(group,"","","", RecomputationsManager.ElementType.GROUP);
        DateTime startRecPeriod = null;
        DateTime endRecPeriod = null;
        DateTime today = Utils.convertStringtoDate("yyyy-MM-dd", runDate);
        today.withTime(0, 0, 0, 0);
        DateTime tomorrow = today.plusDays(1);

        if (changedStatusItem != null) {
            System.out.println("the changed status is not null for : " + group);
            String str = changedStatusItem.getStartPeriod();
            String strend = changedStatusItem.getEndPeriod();
            startRecPeriod = Utils.convertStringtoDate(formatter, changedStatusItem.getStartPeriod());
            endRecPeriod = Utils.convertStringtoDate(formatter, changedStatusItem.getEndPeriod());
            if (startRecPeriod.isBefore(today)) {
                startRecPeriod = today;
            }
            if (!endRecPeriod.isBefore(tomorrow)) {
                endRecPeriod = tomorrow;
            }
        }

//            if (changedStatusItem != null) {
        for (TimeStatus timestatus : timeStatusList) {
            DateTime dt = new DateTime(timestatus.getTimestamp(), DateTimeZone.UTC);
            //  DateTime timestamp = new DateTime(timestatus.getTimestamp());

            if (changedStatusItem != null) {

                System.out.println("the changed status is not null for : " + group);

                if (!dt.isBefore(startRecPeriod) && !dt.isAfter(endRecPeriod)) {
                    timestatus.setStatus(opsMgr.getIntStatus(changedStatusItem.getStatus()));
                    overrideByRecomp = Boolean.TRUE;
                    samples.put(dt, timestatus.getStatus());
                } else {
                    if (dt.isAfter(startRecPeriod)) {
                        if (!samples.containsKey(startRecPeriod)) {
                            overrideByRecomp = Boolean.TRUE;
                            samples.put(startRecPeriod, opsMgr.getIntStatus(changedStatusItem.getStatus()));
                        }
                        if (dt.isAfter(endRecPeriod)) {
                            if (!samples.containsKey(endRecPeriod)) {
                                overrideByRecomp = Boolean.TRUE;
                                samples.put(endRecPeriod, timestatus.getStatus());
                            }
                        }
                    } else {
                        samples.put(dt, timestatus.getStatus());
                    }
                }
            } else {
                samples.put(dt, timestatus.getStatus());
            }
        }
        // Map.Entry<DateTime,Integer> lastEntry= samples.lastEntry();
        Map.Entry<DateTime, Integer> lastEntry = samples.lastEntry();

        if (startRecPeriod != null && changedStatusItem != null && !samples.containsKey(startRecPeriod)) {
            overrideByRecomp = Boolean.TRUE;
            samples.put(startRecPeriod, opsMgr.getIntStatus(changedStatusItem.getStatus()));
        }
        if (endRecPeriod != null && endRecPeriod.isBefore(tomorrow) && changedStatusItem != null && !samples.containsKey(endRecPeriod)) {

            overrideByRecomp = Boolean.TRUE;
            samples.put(endRecPeriod, lastEntry.getValue());
        }
        Timeline timeline = new Timeline();
        timeline.insertDateTimeStamps(samples, true);
        timeline.setOverrideByRecomp(overrideByRecomp);
        return timeline;
    }

}
