package argo.batch;

import argo.avro.GroupEndpoint;
import java.io.IOException;
import java.util.List;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import argo.avro.MetricProfile;
import java.text.ParseException;
import java.util.ArrayList;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.joda.time.DateTimeZone;
import profilesmanager.AggregationProfileManager;
import profilesmanager.EndpointGroupManager;
import profilesmanager.MetricProfileManager;
import profilesmanager.OperationsManager;
import profilesmanager.ReportManager;
import timelines.Utils;

/* Accepts a list o status metrics grouped by the fields: endpoint group,
 * service, endpoint Uses Continuous Timelines and Aggregators to calculate the
 * status results of a service endpoint Prepares the data in a form aligned with
 * the datastore schema for status endpoint collection
 */
public class CalcStatusEndpoint extends RichFlatMapFunction<StatusTimeline, StatusMetric> {

    private static final long serialVersionUID = 1L;

    final ParameterTool params;

    public CalcStatusEndpoint(ParameterTool params) {
        this.params = params;
    }

    static Logger LOG = LoggerFactory.getLogger(ArgoMultiJob.class);

    private List<MetricProfile> mps;
    private List<String> aps;
    private List<String> ops;
    private MetricProfileManager mpsMgr;
    private AggregationProfileManager apsMgr;
    private OperationsManager opsMgr;
    private EndpointGroupManager egpMgr;
    private List<GroupEndpoint> egp;
    private ReportManager repMgr;
    private List<String> conf;

    private String runDate;

    @Override
    public void open(Configuration parameters) throws IOException, ParseException {

        this.runDate = params.getRequired("run.date");
        // Get data from broadcast variables
        this.mps = getRuntimeContext().getBroadcastVariable("mps");
        this.aps = getRuntimeContext().getBroadcastVariable("aps");
        this.ops = getRuntimeContext().getBroadcastVariable("ops");
        // Initialize metric profile manager
        this.mpsMgr = new MetricProfileManager();
        this.mpsMgr.loadFromList(mps);
        // Initialize aggregation profile manager
        this.apsMgr = new AggregationProfileManager();

        this.apsMgr.loadJsonString(aps);
        // Initialize operations manager
        this.opsMgr = new OperationsManager();
        this.opsMgr.loadJsonString(ops);
        this.egp = getRuntimeContext().getBroadcastVariable("egp");
        // Initialize endpoint group manager
        this.egpMgr = new EndpointGroupManager();
        this.egpMgr.loadFromList(egp);
        this.conf = getRuntimeContext().getBroadcastVariable("conf");

        this.repMgr = new ReportManager();
        this.repMgr.loadJsonString(conf);
        this.runDate = params.getRequired("run.date");
    }

    @Override
    public void flatMap(StatusTimeline in, Collector<StatusMetric> out) throws Exception {
        int dateInt = Integer.parseInt(this.runDate.replace("-", ""));
        String function = in.getFunction();
        String service = in.getService();
        String endpointGroup = in.getGroup();
        String hostname = in.getHostname();
        ArrayList<TimeStatus> timestamps = in.getTimestamps();
        boolean hasThr = false;
        if (in.hasThr()) {
            hasThr = true;
        }
        String groupType = this.repMgr.egroup;

        String info = this.egpMgr.getInfo(endpointGroup, groupType, hostname, service);

        for (TimeStatus item : timestamps) {
            StatusMetric cur = new StatusMetric();
            cur.setDateInt(dateInt);
            cur.setGroup(endpointGroup);
            cur.setHostname(hostname);
            cur.setService(service);
            cur.setFunction(function);
            cur.setInfo(info);
            cur.setHasThr(hasThr);
            cur.setTimestamp(Utils.convertDateToString("yyyy-MM-dd'T'HH:mm:ss'Z'", new DateTime(item.getTimestamp(),DateTimeZone.UTC)));
            cur.setStatus(opsMgr.getStrStatus(item.getStatus()));
            out.collect(cur);
        }

    }

}
