package argo.ar;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import argo.batch.StatusTimeline;
import argo.batch.TimeStatus;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import profilesmanager.OperationsManager;
import profilesmanager.RecomputationsManager;
import timelines.Timeline;
import timelines.TimelineAggregator;
import utils.Utils;

/**
 * Accepts a list of monitoring timelines and produces an endpoint timeline The
 * class is used as a RichGroupReduce Function in flink pipeline
 */
public class ExcludeGroupMetrics extends RichFlatMapFunction<StatusTimeline, StatusTimeline> {

    private static final long serialVersionUID = 1L;

    final ParameterTool params;

    public ExcludeGroupMetrics(ParameterTool params) {
        this.params = params;
    }

    static Logger LOG = LoggerFactory.getLogger(ExcludeGroupMetrics.class);
    private List<String> rec;
    private RecomputationsManager recMgr;
    private List<String> ops;
    private OperationsManager opsMgr;

    private String runDate;

    /**
     * Initialization method of the RichGroupReduceFunction operator
     * <p>
     * This runs at the initialization of the operator and receives a
     * configuration parameter object. It initializes all required structures
     * used by this operator such as profile managers, operations managers,
     * topology managers etc.
     *
     * @param parameters A flink Configuration object
     */
    @Override
    public void open(Configuration parameters) throws IOException, ParseException {
        // Get data from broadcast variables
        this.rec = getRuntimeContext().getBroadcastVariable("rec");
        // Initialize metric profile manager
        this.recMgr = new RecomputationsManager();
        this.recMgr.loadJsonString(rec);
        // Initialize aggregation profile manager
        this.ops = getRuntimeContext().getBroadcastVariable("ops");
        // Initialize operations manager
        this.opsMgr = new OperationsManager();
        this.opsMgr.loadJsonString(ops);

        this.runDate = params.getRequired("run.date");

    }

    /**
     * The main operator business logic of transforming a collection of
     * MetricTimelines to an aggregated endpoint timeline
     * <p>
     * This runs for each group item (endpointGroup,service,hostname) and
     * contains a list of metric timelines sorted by the "metric" field. It uses
     * a Discrete Aggregator to aggregate the metric timelines according to the
     * operations profile defined in the Operations Manager as to produce the
     * final Endpoint Timeline. The type of metric aggregation is defined in the
     * aggregation profile managed by the AggregationManager
     *
     * @param in An Iterable collection of MonTimeline objects
     * @param out A Collector list of MonTimeline to acquire the produced
     * endpoint timelines.
     */
    @Override
    public void flatMap(StatusTimeline in, Collector<StatusTimeline> out) throws Exception {
        DateTime runDateDt = Utils.convertStringtoDate("yyyy-MM-dd", this.runDate);
        // Initialize field values and aggregator
        HashMap<String, Timeline> timelinelist = new HashMap<>();

        if (this.recMgr.isExcluded(in.getGroup())) {
            TreeMap<DateTime, Integer> samples = new TreeMap<>();
            for (TimeStatus timestatus : in.getTimestamps()) {
                DateTime dt = new DateTime(timestatus.getTimestamp());
                samples.put(dt, timestatus.getStatus());
            }

            Timeline timeline = new Timeline();
            timeline.insertDateTimeStamps(samples, true);
            timelinelist.put(in.getGroup(), timeline);

            TimelineAggregator timelineAggr = new TimelineAggregator(timelinelist);
            timelineAggr.fillUnknown(in.getGroup(), runDate, this.opsMgr.getDefaultUnknownInt());

            Timeline exludedTimeline = timelineAggr.getOutput();
            ArrayList<TimeStatus> timestatuCol = new ArrayList();
            for (Map.Entry<DateTime, Integer> entry : exludedTimeline.getSamples()) {
                TimeStatus timestatus = new TimeStatus(entry.getKey().getMillis(), entry.getValue());
                timestatuCol.add(timestatus);
            }

            in.setTimestamps(timestatuCol);

        }
        out.collect(in);

    }

    public static double round(double input, int prec, int mode) {
        try {
            BigDecimal inputBD = BigDecimal.valueOf(input);
            BigDecimal rounded = inputBD.setScale(prec, mode);
            return rounded.doubleValue();

        } catch (NumberFormatException e) {
            return -1;
        }
    }

}
