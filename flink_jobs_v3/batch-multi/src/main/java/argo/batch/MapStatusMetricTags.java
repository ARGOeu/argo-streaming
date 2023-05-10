package argo.batch;

import argo.avro.GroupEndpoint;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import profilesmanager.EndpointGroupManager;
import profilesmanager.MetricTagsManager;
import profilesmanager.ReportManager;

/**
 * MapStatusMetricTags produces StatusMetric enriched with the defined tags that
 * correspond to the metric
 */
public class MapStatusMetricTags extends RichFlatMapFunction<StatusMetric, StatusMetric> {

    public MapStatusMetricTags(boolean isTrendsTags) {

        this.isTrendsTags = isTrendsTags;
    }

    List<String> mtags;
    private MetricTagsManager mtagsMgr;

    private EndpointGroupManager egpMgr;
    private List<GroupEndpoint> egp;
    private ReportManager repMgr;
    private List<String> conf;
    private boolean isTrendsTags;

    @Override
    public void open(Configuration parameters) throws IOException {
        // Initialize MetricTags manager          
        this.mtags = getRuntimeContext().getBroadcastVariable("mtags");
        this.mtagsMgr = new MetricTagsManager();
        this.mtagsMgr.loadJsonString(mtags);
        this.egp = getRuntimeContext().getBroadcastVariable("egp");

        // Initialize endpoint group manager
        this.egpMgr = new EndpointGroupManager();
        this.egpMgr.loadFromList(egp);
        this.conf = getRuntimeContext().getBroadcastVariable("conf");

        this.repMgr = new ReportManager();
        this.repMgr.loadJsonString(conf);

    }

    /**
     * if the service exist in one or more function groups , timeline trends are
     * produced for each function that the service belongs and the function info
     * is added to the timelinetrend
     *
     * @param t
     * @param out
     * @throws Exception
     */
    @Override
    public void flatMap(StatusMetric t, Collector<StatusMetric> out) throws Exception {
        String tagInfo = "";

        if (isTrendsTags) {
            ArrayList<String> tags = this.mtagsMgr.getTags(t.getMetric());

            for (String tag : tags) {
                if (tags.indexOf(tag) == 0) {
                    tagInfo = tagInfo + tag;
                } else {
                    tagInfo = tagInfo + "," + tag;
                }
            }
        }
        StatusMetric newT = t;
        newT.setTags(tagInfo);
        if (!t.getOgStatus().equals("")) {
            newT.setHasThr(true);
        }

        String groupType = this.repMgr.egroup;

        String info = this.egpMgr.getInfo(t.getGroup(), groupType, t.getHostname(), t.getService());
        newT.setInfo(info);
        out.collect(newT);

    }

}
