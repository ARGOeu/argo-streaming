package argo.batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.flink.api.common.functions.RichFlatMapFunction;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profilesmanager.AggregationProfileManager;
import profilesmanager.OperationsManager;

import timelines.TimelineAggregator;

/**
 * Accepts a list o status metrics grouped by the fields: endpoint group Uses
 * Continuous Timelines and Aggregators to calculate the status results of an
 * endpoint group Prepares the data in a form aligned with the datastore schema
 * for status flavor collection
 */
public class CalcStatusEndGroup extends RichFlatMapFunction<StatusTimeline, StatusMetric> {

    private static final long serialVersionUID = 1L;

    final ParameterTool params;

    public CalcStatusEndGroup(ParameterTool params) {
        this.params = params;
    }

    static Logger LOG = LoggerFactory.getLogger(ArgoStatusBatch.class);

    private List<String> aps;
    private List<String> ops;

    private AggregationProfileManager apsMgr;
    private OperationsManager opsMgr;

    private String runDate;
    public HashMap<String, TimelineAggregator> groupEndpointAggr;

    private boolean getGroup;

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

        // Initialize endpoint group type
        this.runDate = params.getRequired("run.date");
        // set the Structures
        this.groupEndpointAggr = new HashMap<String, TimelineAggregator>();

        this.getGroup = true;
    }

    @Override
    public void flatMap(StatusTimeline in, Collector<StatusMetric> out) throws Exception {

        int dateInt = Integer.parseInt(this.runDate.replace("-", ""));

        String endpointGroup = in.getGroup();
        ArrayList<TimeStatus> timestamps = in.getTimestamps();

        for (TimeStatus item : timestamps) {
            StatusMetric cur = new StatusMetric();
            cur.setDateInt(dateInt);
            cur.setGroup(endpointGroup);
            cur.setTimestamp(utils.Utils.convertDateToString("yyyy-MM-dd'T'HH:mm:ss'Z'", new DateTime(item.getTimestamp())));

            cur.setStatus(opsMgr.getStrStatus(item.getStatus()));

            out.collect(cur);
        }

    }

}
