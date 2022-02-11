package argo.batch;

import java.util.List;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
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
import utils.Utils;

public class CalcPrevStatus extends RichGroupReduceFunction<StatusMetric, StatusMetric> {

    private static final long serialVersionUID = 1L;

    final ParameterTool params;

    public CalcPrevStatus(ParameterTool params) {
        this.params = params;
    }

    static Logger LOG = LoggerFactory.getLogger(ArgoStatusBatch.class);

    private List<MetricProfile> mps;
    private List<GroupEndpoint> egp;
    private List<GroupGroup> ggp;
    private MetricProfileManager mpsMgr;
    private String runDate;
    private List<String> rec;
    private RecomputationsManager recMgr;
    private List<String> ops;
    private OperationsManager opsMgr;

    @Override
    public void open(Configuration parameters) throws IOException, ParseException {
        // Get data from broadcast variable
        this.runDate = params.getRequired("run.date");

        this.mps = getRuntimeContext().getBroadcastVariable("mps");
        this.rec = getRuntimeContext().getBroadcastVariable("rec");
        this.ops = getRuntimeContext().getBroadcastVariable("ops");

        // Initialize metric profile manager
        this.mpsMgr = new MetricProfileManager();
        this.mpsMgr.loadFromList(mps);
        // Initialize endpoint group manager
        this.recMgr = new RecomputationsManager();
        this.recMgr.loadJsonString(rec);
        this.opsMgr = new OperationsManager();
        this.opsMgr.loadJsonString(ops);

    }

    @Override
    public void reduce(Iterable<StatusMetric> in, Collector<StatusMetric> out) throws Exception {
        // group input is sorted 
        String prevStatus = "MISSING";
        String prevTimestamp = this.runDate + "T00:00:00Z";
        boolean gotPrev = false;
        for (StatusMetric item : in) {
            // If haven't captured yet previous timestamp

            if (!gotPrev) {
                if (item.getTimestamp().split("T")[0].compareToIgnoreCase(this.runDate) != 0) {
                    // set prevTimestamp to this
                    prevTimestamp = item.getTimestamp();
                    prevStatus = item.getStatus();
                    gotPrev = true;
                    continue;
                }
            }

            item.setPrevState(prevStatus);
            item.setPrevTs(prevTimestamp);
            if (item.getTimestamp().split("T")[0].compareToIgnoreCase(this.runDate) == 0) {
                if (!item.getOgStatus().equals("")) {
                    item.setHasThr(true);
                }
                RecomputationsManager.ExcludedMetric excludedMetric = this.recMgr.findMetricExcluded(item.getGroup(), item.getService(), item.getHostname(), item.getMetric());
                if (excludedMetric != null) {
                    String status = item.getStatus();
                    DateTime today = Utils.convertStringtoDate("yyyy-MM-dd", runDate);
                    today.withTime(0, 0, 0, 0);
                    DateTime tomorrow = today.plusDays(1);

                    DateTime timestamp = Utils.convertStringtoDate("yyyy-MM-dd'T'HH:mm:ss'Z'", item.getTimestamp());
                    DateTime startPeriod = Utils.convertStringtoDate("yyyy-MM-dd'T'HH:mm:ss'Z'", excludedMetric.getStartPeriod());
                    DateTime endPeriod = Utils.convertStringtoDate("yyyy-MM-dd'T'HH:mm:ss'Z'", excludedMetric.getEndPeriod());
                    if (!timestamp.isBefore(startPeriod) && !timestamp.isAfter(endPeriod)) {
                        item.setStatus(this.opsMgr.getDefaultExcludedState());
                        out.collect(item);

                        if (!endPeriod.isBefore(today) && !endPeriod.isAfter(tomorrow)) {
                            item.setTimestamp(endPeriod.plusSeconds(1).toString("yyyy-MM-dd'T'HH:mm:ss'Z'"));

                            String timestamp2 = item.getTimestamp().split("Z")[0];
                            String[] tsToken = timestamp2.split("T");
                            int timeInt = Integer.parseInt(tsToken[1].replace(":", ""));

                            item.setStatus(status);
                            item.setTimeInt(timeInt);
                            item.setPrevState(this.opsMgr.getDefaultExcludedState());
                            item.setPrevTs(today.toString("yyyy-MM-dd'T'HH:mm:ss'Z'"));
                            out.collect(item);
                        }
                        if (!startPeriod.isBefore(today) && !startPeriod.isAfter(tomorrow)) {
                            item.setStatus(status);
                            item.setPrevState(prevStatus);
                            item.setPrevTs(prevTimestamp);
                            item.setTimestamp(startPeriod.plusSeconds(1).toString("yyyy-MM-dd'T'HH:mm:ss'Z'"));
                            String timestamp2 = item.getTimestamp().split("Z")[0];
                            String[] tsToken = timestamp2.split("T");
                            int timeInt = Integer.parseInt(tsToken[1].replace(":", ""));
                            item.setTimeInt(timeInt);
                            out.collect(item);
                        }
                    }
                } else {
                    out.collect(item);
                }
            }

            prevStatus = item.getStatus();
            prevTimestamp = item.getTimestamp();
        }

    }

}
