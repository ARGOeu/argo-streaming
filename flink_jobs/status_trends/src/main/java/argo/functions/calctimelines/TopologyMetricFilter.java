/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.functions.calctimelines;

import argo.avro.MetricData;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.RichFilterFunction;

/**
 *
 * @author cthermolia
 *
 * TopologyMetricFilter , filters service endpoint and exclude the ones that do
 * not appear in topology and metric profile data inputs
 */
public class TopologyMetricFilter implements FilterFunction<MetricData> {

    private HashMap<String, String> groupEndpoints;
    private HashMap<String, ArrayList<String>> metricProfileData;

    public TopologyMetricFilter(HashMap<String, ArrayList<String>> metricProfileData, HashMap<String, String> groupEndpoints) {
        this.metricProfileData = metricProfileData;
        this.groupEndpoints = groupEndpoints;

    }

    @Override
    public boolean filter(MetricData t) throws Exception {
        String group = this.groupEndpoints.get(t.getHostname().toString() + "-" + t.getService().toString()); //retrieve the group for the service, as contained in file group_endpoints. if group is null exit 
        boolean hasGroup = false, hasMetric = false;
        if (group != null) {
            hasGroup = true;
        }
        if (hasGroup && metricProfileData.get(t.getService().toString()) != null && metricProfileData.get(t.getService().toString()).contains(t.getMetric().toString())) { //if metric is contained in file metrics_profile_data 
           return true;
        }
        
        
        return false;
    }

}
