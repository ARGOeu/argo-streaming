/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.functions.calctimelines;

import argo.avro.MetricData;
import argo.profiles.AggregationProfileManager;
import argo.profiles.EndpointGroupManager;
import argo.profiles.GroupGroupManager;
import argo.profiles.MetricProfileManager;

import argo.profiles.TopologyGroupParser;
import java.util.ArrayList;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * TopologyMetricFilter , filters service endpoint and exclude the ones that do
 * not appear in topology and metric profile data inputs
 */
public class TopologyMetricFilter implements FilterFunction<MetricData> {

    private MetricProfileManager metricProfileParser;
    private EndpointGroupManager topologyEndpointParser;
    private GroupGroupManager topologyGroupParser;
    private AggregationProfileManager aggregationProfileParser;

    public TopologyMetricFilter(MetricProfileManager metricProfileParser, EndpointGroupManager topologyEndpointParser, GroupGroupManager topologyGroupParser, AggregationProfileManager aggregationProfileParser) {
        this.metricProfileParser = metricProfileParser;
        this.topologyEndpointParser = topologyEndpointParser;
        this.topologyGroupParser = topologyGroupParser;
        this.aggregationProfileParser = aggregationProfileParser;

    }

    @Override
    public boolean filter(MetricData t) throws Exception {
        String avProfileName = this.aggregationProfileParser.getAvProfileItem().getName();

        ArrayList<String> groups = topologyEndpointParser.getGroupFull(aggregationProfileParser.getProfileGroupType(avProfileName).toUpperCase(), t.getHostname().toString(), t.getService().toString());
       for (String group : groups) {
            if (topologyGroupParser.checkSubGroup(group) && metricProfileParser.containsMetric(t.getService().toString(), t.getMetric().toString())) {
                return true;
            }
        }
        return false;

    }
}
