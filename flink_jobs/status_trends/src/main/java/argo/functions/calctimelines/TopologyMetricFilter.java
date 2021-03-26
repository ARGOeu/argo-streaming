/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.functions.calctimelines;

import argo.avro.MetricData;
import argo.profiles.AggregationProfileParser;
import argo.profiles.MetricProfileParser;
import argo.profiles.TopologyEndpointParser;
import argo.profiles.TopologyGroupParser;
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

//    private HashMap<String, String> topologyEndpoints;
//    private HashMap<String, ArrayList<String>> metricProfileData;
//
//    private ArrayList<String> topologyGroups;
//    
    private MetricProfileParser metricProfileParser;
    private TopologyEndpointParser topologyEndpointParser;
    private TopologyGroupParser topologyGroupParser;
    private AggregationProfileParser aggregationProfileParser;

    public TopologyMetricFilter(MetricProfileParser metricProfileParser, TopologyEndpointParser topologyEndpointParser, TopologyGroupParser topologyGroupParser, AggregationProfileParser aggregationProfileParser) {
        this.metricProfileParser = metricProfileParser;
        this.topologyEndpointParser = topologyEndpointParser;
        this.topologyGroupParser = topologyGroupParser;
        this.aggregationProfileParser=aggregationProfileParser;
    }

//    public TopologyMetricFilter(HashMap<String, ArrayList<String>> metricProfileData, HashMap<String, String> topologyEndpoints, ArrayList<String> topologyGroups) {
//        this.metricProfileData = metricProfileData;
//        this.topologyEndpoints = topologyEndpoints;
//        this.topologyGroups = topologyGroups;
//    }
    @Override
    public boolean filter(MetricData t) throws Exception {

        String group=topologyEndpointParser.retrieveGroup(aggregationProfileParser.getEndpointGroup().toUpperCase(), t.getHostname() + "-" + t.getService());
//        String group = this.topologyEndpoints.get(t.getHostname().toString() + "-" + t.getService().toString()); //retrieve the group for the service, as contained in file group_endpoints. if group is null exit 
        boolean hasGroup = false;
         if (topologyGroupParser.containsGroup(group) && group != null) {
            hasGroup = true;
        }
        if (hasGroup && metricProfileParser.containsMetric(t.getService().toString(),t.getMetric().toString())){

            return true;
        }

//        if (this.topologyGroups.contains(group) && group != null) {
//            hasGroup = true;
//        }
//        if (hasGroup && metricProfileData.get(t.getService().toString()) != null && metricProfileData.get(t.getService().toString()).contains(t.getMetric().toString())) { //if metric is contained in file metrics_profile_data 
//
//            return true;
//        }
//
        return false;
    }
}
