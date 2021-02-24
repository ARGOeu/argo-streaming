/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.functions.timeline;

import argo.avro.MetricData;
import java.util.ArrayList;
import java.util.HashMap;
import argo.utils.Utils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

/**
 *
 * @author cthermolia
 *
 * TopologyMetricFilter , filters service endpoint and exclude the ones that do
 * not appear in topology and metric profile data inputs
 */
public class TopologyMetricFilter extends RichFilterFunction<MetricData> {

    private transient HashMap<String, String> groupEndpoints;
    private HashMap<String, ArrayList<String>> metricProfileData;
    private String groupEndpointsPath;

    public TopologyMetricFilter(HashMap<String, ArrayList<String>> metricProfileData, String groupEndpointsPath) {
        this.metricProfileData = metricProfileData;
        this.groupEndpointsPath = groupEndpointsPath;

    }

    @Override
    public void open(Configuration config) throws Exception {
        groupEndpoints = Utils.readGroupEndpointJson(groupEndpointsPath); //contains the information of the (group, service) matches
    }

    @Override
    public boolean filter(MetricData t) throws Exception {
        String group = groupEndpoints.get(t.getHostname().toString() + "-" + t.getService().toString()); //retrieve the group for the service, as contained in file group_endpoints. if group is null exit 
        boolean hasGroup = false, hasMetric = false;
        if (group != null) {
            hasGroup = true;
        }
        if (metricProfileData.get(t.getService().toString()) != null && metricProfileData.get(t.getService().toString()).contains(t.getMetric().toString())) { //if metric is contained in file metrics_profile_data 
            hasMetric = true;
        }
        if (hasGroup && hasMetric) {
            return true;
        }
        return false;
    }

}
