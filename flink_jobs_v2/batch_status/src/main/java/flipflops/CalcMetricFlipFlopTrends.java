package flipflops;




/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import argo.batch.StatusTimeline;
import argo.batch.TimeStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profilesmanager.OperationsManager;
import timelines.Timeline;



/**
 * Accepts a list of monitoring timelines and produces an endpoint timeline The
 * class is used as a RichGroupReduce Function in flink pipeline
 */
public class CalcMetricFlipFlopTrends extends RichFlatMapFunction<StatusTimeline, MetricTrends> {
    private static final long serialVersionUID = 1L;

  
    public CalcMetricFlipFlopTrends() {
   
    }

    static Logger LOG = LoggerFactory.getLogger(CalcMetricFlipFlopTrends.class);
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
    public void open(Configuration parameters) throws IOException {
        this.ops = getRuntimeContext().getBroadcastVariable("ops");
         // Initialize operations manager
        this.opsMgr = new OperationsManager();
        this.opsMgr.loadJsonString(ops);

     
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
    public void flatMap(StatusTimeline in, Collector<MetricTrends> out) throws Exception {
       // Initialize field values and aggregator
        String metric = "";
        String hostname = "";
        String service = "";
        String endpointGroup = "";
        metric=in.getMetric();
        hostname=in.getHostname();
        service = in.getService();
        endpointGroup = in.getGroup();
        ArrayList<TimeStatus> timestatusList = in.getTimestamps();

        TreeMap<DateTime, Integer> timestampMap = new TreeMap();
        for (TimeStatus ts : timestatusList) {
            timestampMap.put(new DateTime(ts.getTimestamp()), ts.getStatus());
        }

        Timeline timeline = new Timeline();
        timeline.insertDateTimeStamps(timestampMap, true);
        HashMap<String, Timeline> timelineMap = new HashMap<>();
        timelineMap.put("timeline", timeline);      

        Integer flipflop = timeline.calcStatusChanges();

        if (endpointGroup != null && service != null && hostname != null && metric != null) {

            MetricTrends metricTrends = new MetricTrends(endpointGroup, service, hostname, metric, timeline, flipflop);
            out.collect(metricTrends);
        }
    }

}