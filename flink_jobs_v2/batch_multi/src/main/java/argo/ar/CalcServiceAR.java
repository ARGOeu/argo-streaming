package argo.ar;

import java.io.IOException;
import java.util.List;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;

import argo.avro.MetricProfile;

import argo.batch.StatusTimeline;
import argo.batch.TimeStatus;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import profilesmanager.AggregationProfileManager;
import profilesmanager.EndpointGroupManager;
import profilesmanager.GroupGroupManager;
import profilesmanager.MetricProfileManager;
import profilesmanager.OperationsManager;
import profilesmanager.ReportManager;
import timelines.Timeline;
import timelines.TimelineIntegrator;
import utils.Utils;

/**
 * Accepts a list of monitoring timelines and produces an endpoint timeline The
 * class is used as a RichGroupReduce Function in flink pipeline
 */
public class CalcServiceAR extends RichFlatMapFunction<StatusTimeline, ServiceAR> {

    private static final long serialVersionUID = 1L;

    final ParameterTool params;

    public CalcServiceAR(ParameterTool params) {
        this.params = params;
    }

    static Logger LOG = LoggerFactory.getLogger(CalcEndpointAR.class);
    private List<String> conf;
    private List<MetricProfile> mps;
    private List<String> aps;
    private List<String> ops;
    private List<GroupEndpoint> egp;
    private List<GroupGroup> ggp;
    private MetricProfileManager mpsMgr;
    private AggregationProfileManager apsMgr;
    private EndpointGroupManager egpMgr;
    private GroupGroupManager ggpMgr;
    private OperationsManager opsMgr;
    private ReportManager repMgr;
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
        this.mps = getRuntimeContext().getBroadcastVariable("mps");
        this.aps = getRuntimeContext().getBroadcastVariable("aps");
        this.ops = getRuntimeContext().getBroadcastVariable("ops");
        this.egp = getRuntimeContext().getBroadcastVariable("egp");
        this.ggp = getRuntimeContext().getBroadcastVariable("ggp");
        this.conf = getRuntimeContext().getBroadcastVariable("conf");
        // Initialize metric profile manager
        this.mpsMgr = new MetricProfileManager();
        this.mpsMgr.loadFromList(mps);
        // Initialize aggregation profile manager
        this.apsMgr = new AggregationProfileManager();

        this.apsMgr.loadJsonString(aps);
        // Initialize operations manager
        this.opsMgr = new OperationsManager();
        this.opsMgr.loadJsonString(ops);

        // Initialize endpoint group manager
        this.egpMgr = new EndpointGroupManager();
        this.egpMgr.loadFromList(egp);

        this.ggpMgr = new GroupGroupManager();
        this.ggpMgr.loadFromList(ggp);
        this.repMgr = new ReportManager();
        this.repMgr.loadJsonString(conf);

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
    public void flatMap(StatusTimeline in, Collector<ServiceAR> out) throws Exception {
        DateTime runDateDt = Utils.convertStringtoDate("yyyy-MM-dd", this.runDate);
        // Initialize field values and aggregator
        String service = "";
        String endpointGroup = "";
        service = in.getService();
        endpointGroup = in.getGroup();
        
//        // Availability = UP period / KNOWN period = UP period / (Total period –
//		// UNKNOWN period)
//		this.availability = round(((up / dt) / (1.0 - (unknown / dt))) * 100, 5, BigDecimal.ROUND_HALF_UP);
//
//		// Reliability = UP period / (KNOWN period – Scheduled Downtime)
//		// = UP period / (Total period – UNKNOWN period – ScheduledDowntime)
//		this.reliability = round(((up / dt) / (1.0 - (unknown / dt) - (down / dt))) * 100, 5, BigDecimal.ROUND_HALF_UP);
//
//		this.up_f = round(up / dt, 5, BigDecimal.ROUND_HALF_UP);
//		this.unknown_f = round(unknown / dt, 5, BigDecimal.ROUND_HALF_UP);
//		this.down_f = round(down / dt, 5, BigDecimal.ROUND_HALF_UP);
        ArrayList<TimeStatus> timestatusList = in.getTimestamps();

        TreeMap<DateTime, Integer> timestampMap = new TreeMap();
        for (TimeStatus ts : timestatusList) {
            timestampMap.put(new DateTime(ts.getTimestamp(), DateTimeZone.UTC), ts.getStatus());
        }

        Timeline timeline = new Timeline();
        timeline.insertDateTimeStamps(timestampMap, true);
        HashMap<String, Timeline> timelineMap = new HashMap<>();
        timelineMap.put("timeline", timeline);

        TimelineIntegrator tIntegrator = new TimelineIntegrator();

        tIntegrator.calcAR(timeline.getSamples(), runDateDt, this.opsMgr.getIntStatus("OK"), this.opsMgr.getIntStatus("WARNING"), this.opsMgr.getDefaultUnknownInt(), this.opsMgr.getDefaultDownInt(), this.opsMgr.getDefaultMissingInt());

        int runDateInt = Integer.parseInt(this.runDate.replace("-", ""));
        ServiceAR result = new ServiceAR(runDateInt, this.repMgr.id, service, endpointGroup, tIntegrator.getAvailability(), tIntegrator.getReliability(), tIntegrator.getUp_f(), tIntegrator.getUnknown_f(), tIntegrator.getDown_f());
        // Output MonTimeline object
        out.collect(result);

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
