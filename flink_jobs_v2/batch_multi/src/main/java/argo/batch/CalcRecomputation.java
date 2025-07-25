package argo.batch;

import java.util.*;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hdfs.server.namenode.HostFileManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;

import argo.avro.MetricProfile;

import java.io.IOException;
import java.text.ParseException;

import org.joda.time.DateTime;
import profilesmanager.MetricProfileManager;
import profilesmanager.OperationsManager;
import profilesmanager.RecomputationsManager;
import utils.RecompTimelineBuilder;
import utils.Utils;

//transforms the original status to the excluded status in the case the metric is inside the exclude period
public class CalcRecomputation extends RichGroupReduceFunction<StatusMetric, StatusMetric> {

    private static final long serialVersionUID = 1L;

    final ParameterTool params;

    public CalcRecomputation(ParameterTool params) {
        this.params = params;
    }

    static Logger LOG = LoggerFactory.getLogger(ArgoMultiJob.class);
    private List<MetricProfile> mps;
    private List<GroupEndpoint> egp;
    private List<GroupGroup> ggp;
    private MetricProfileManager mpsMgr;
    private String runDate;
    private List<HashMap<String, List<RecomputationsManager.RecomputationElement>>> rec;
    private List<String> ops;
    private OperationsManager opsMgr;
    @Override
    public void open(Configuration parameters) throws IOException, ParseException {
        // Get data from broadcast variable
        this.runDate = params.getRequired("run.date");

        this.mps = getRuntimeContext().getBroadcastVariable("mps");
        this.rec = getRuntimeContext().getBroadcastVariable("rec");
        RecomputationsManager.metricRecomputationItems=this.rec.get(0);
        this.ops = getRuntimeContext().getBroadcastVariable("ops");

        // Initialize metric profile manager
        this.mpsMgr = new MetricProfileManager();
        this.mpsMgr.loadFromList(mps);
        this.opsMgr = new OperationsManager();
        this.opsMgr.loadJsonString(ops);

    }

    @Override
    public void reduce(Iterable<StatusMetric> in, Collector<StatusMetric> out) throws Exception {
        int i = 0;
        String service = "";
        String function = "";
        String endpointGroup = "";
        String hostname = "";
        String metric = "";
        boolean hasThr = false;
        TreeMap<DateTime, StatusMetric> timeStatusMap = new TreeMap<>();
        DateTime today = Utils.convertStringtoDate("yyyy-MM-dd", runDate);
        today = today.withTime(0, 0, 0, 0);


        for (StatusMetric item : in) {
            service = item.getService();
            endpointGroup = item.getGroup();
            hostname = item.getHostname();
            function = item.getFunction();
            metric = item.getMetric();
            String ts = item.getTimestamp();
            String[] tsToken = item.getTimestamp().split("Z")[0].split("T");
            item.setDateInt(Integer.parseInt(tsToken[0].replace("-", "")));
            item.setTimeInt(Integer.parseInt(tsToken[1].replace(":", "")));

            timeStatusMap.put(Utils.convertStringtoDate("yyyy-MM-dd'T'HH:mm:ss'Z'", ts), item);

        }

        ArrayList<RecomputationsManager.RecomputationElement> recompItems = RecomputationsManager.findChangedStatusItem(
                endpointGroup, // The endpoint group associated with the metric
                service,       // The service associated with the metric
                hostname,      // The hostname for the metric
                metric,        // The metric for which recomputation is being checked
                RecomputationsManager.ElementType.METRIC // Element type, indicating this is a metric-related recomputation
        );

        if (!recompItems.isEmpty()) { // If a recomputation request is found for this metric
            for (RecomputationsManager.RecomputationElement recompItem : recompItems) {
                timeStatusMap = RecompTimelineBuilder.calcRecomputationsMetrics(
                        timeStatusMap, // Current samples of the timeline
                        recompItem,               // The recomputation item with updated status information
                        runDate,                  // The current date for running the recomputation
                        this.opsMgr               // Operations manager to interpret the recomputation's status
                );
            }
        }
        for (
                DateTime dt : timeStatusMap.keySet()) {
            out.collect(timeStatusMap.get(dt));
        }
    }
}