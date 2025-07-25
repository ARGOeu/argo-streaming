package argo.batch;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;
import argo.avro.MetricData;
import argo.avro.MetricProfile;
import profilesmanager.AggregationProfileManager;
import profilesmanager.EndpointGroupManager;
import profilesmanager.GroupGroupManager;
import profilesmanager.MetricProfileManager;
import profilesmanager.OperationsManager;
import profilesmanager.RecomputationsManager;
import profilesmanager.ReportManager;
import profilesmanager.ThresholdManager;

/**
 * Accepts a metric data entry and converts it to a status metric object by
 * appending endpoint group information,filters out entries that do not appear
 * in topology and metric profiles
 */
public class PickEndpoints extends RichFlatMapFunction<MetricData, StatusMetric> {

    private static final long serialVersionUID = 1L;

    final ParameterTool params;

    
    public PickEndpoints(ParameterTool params) {
        this.params = params;
    }

    static Logger LOG = LoggerFactory.getLogger(ArgoMultiJob.class);

    private List<MetricProfile> mps;
    private List<GroupEndpoint> egp;
    private List<GroupGroup> ggp;
    private List<Map<String, ArrayList<Map<String, Date>>>> rec;
    private List<String> cfg;
    private List<String> thr;
    private List<String> ops;
    private List<String> aps;
    private OperationsManager opsMgr;
    private MetricProfileManager mpsMgr;
    private EndpointGroupManager egpMgr;
    private GroupGroupManager ggpMgr;
    private ReportManager cfgMgr;
    private ThresholdManager thrMgr;
    private AggregationProfileManager apsMgr;
    private String egroupType;

    @Override
    public void open(Configuration parameters) throws IOException, ParseException {
        // Get data from broadcast variable
        this.mps = getRuntimeContext().getBroadcastVariable("mps");
        this.egp = getRuntimeContext().getBroadcastVariable("egp");
        this.ggp = getRuntimeContext().getBroadcastVariable("ggp");
        this.ggp = getRuntimeContext().getBroadcastVariable("ggp");
     ///   this.rec = getRuntimeContext().getBroadcastVariable("rec");
        this.cfg = getRuntimeContext().getBroadcastVariable("conf");
        this.thr = getRuntimeContext().getBroadcastVariable("thr");
        this.ops = getRuntimeContext().getBroadcastVariable("ops");
        this.aps = getRuntimeContext().getBroadcastVariable("aps");
        this.rec= getRuntimeContext().getBroadcastVariable("rec");
        RecomputationsManager.monEngines=this.rec.get(0);

        // Initialize metric profile manager
        this.mpsMgr = new MetricProfileManager();
        this.mpsMgr.loadFromList(mps);
        // Initialize endpoint group manager
        this.egpMgr = new EndpointGroupManager();
        this.egpMgr.loadFromList(egp);

        this.ggpMgr = new GroupGroupManager();
        this.ggpMgr.loadFromList(ggp);

        // Initialize report configuration manager
        this.cfgMgr = new ReportManager();
        this.cfgMgr.loadJsonString(cfg);

        // Initialize Ops Manager
        this.opsMgr = new OperationsManager();
        this.opsMgr.loadJsonString(ops);

        // Initialize Aggregation Profile manager
        this.apsMgr = new AggregationProfileManager();
        this.apsMgr.loadJsonString(aps);

        this.egroupType = cfgMgr.egroup;

        // Initialize Threshold manager
        this.thrMgr = new ThresholdManager();
        if (!this.thr.get(0).isEmpty()) {
            this.thrMgr.parseJSON(this.thr.get(0));
        }
  
    }

    @Override
    public void flatMap(MetricData md, Collector<StatusMetric> out) throws Exception {

        String prof = mpsMgr.getProfiles().get(0);
        String aprof = apsMgr.getAvProfiles().get(0);
        String hostname = md.getHostname();
        String service = md.getService();
        String metric = md.getMetric();
        String monHost = md.getMonitoringHost();
        String ts = md.getTimestamp();

        // Filter By monitoring engine
        if (RecomputationsManager.isMonExcluded(monHost, ts)) {
            return;
        }

        // Filter By aggregation profile
        if (!apsMgr.checkService(aprof, service)) {
            return;
        }

        // Filter By metric profile
        if (!mpsMgr.checkProfileServiceMetric(prof, service, metric)) {
            return;
        }

        // Filter By endpoint group if belongs to supergroup
        ArrayList<String> groupnames = egpMgr.getGroup(egroupType, hostname, service);

        for (String groupname : groupnames) {
            if (ggpMgr.checkSubGroup(groupname)) {
                // Create a StatusMetric output
                String timestamp2 = md.getTimestamp().split("Z")[0];
                String[] tsToken = timestamp2.split("T");
                int dateInt = Integer.parseInt(tsToken[0].replace("-", ""));
                int timeInt = Integer.parseInt(tsToken[1].replace(":", ""));
                String status = md.getStatus();
                String actualData = md.getActualData();
                String ogStatus = "";
                String ruleApplied = "";

                 if (actualData != null) {
                    // Check for relevant rule
                    String rule = thrMgr.getMostRelevantRule(groupname, md.getHostname(), md.getMetric());
                    // if rule is indeed found 
                    if (rule != "") {
                        // get the retrieved values from the actual data
                        Map<String, Float> values = thrMgr.getThresholdValues(actualData);
                        // calculate 
                        String[] statusNext = thrMgr.getStatusByRuleAndValues(rule, this.opsMgr, "AND", values);
                        if (statusNext[0] == "") {
                            statusNext[0] = status;
                        }
                        LOG.info("{},{},{} data:({}) {} --> {}", groupname, md.getHostname(), md.getMetric(), values, status, statusNext[0]);
                        if (status != statusNext[0]) {
                             ogStatus = status;
                            ruleApplied = statusNext[1];
                            status = statusNext[0];
                        }
                    }

                }
                String info = this.egpMgr.getInfo(groupname, egroupType, md.getHostname(), md.getService());

                StatusMetric sm = new StatusMetric(groupname, "", md.getService(), md.getHostname(), md.getMetric(), status, md.getTimestamp(), dateInt, timeInt, md.getSummary(), md.getMessage(), "", "", actualData, ogStatus, ruleApplied, info, "");
                out.collect(sm);
            }

        }

    }
}
