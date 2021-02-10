/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.functions;

import argo.avro.MetricData;
import java.util.ArrayList;
import java.util.HashMap;
import argo.utils.Utils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFilterFunction;

import org.apache.flink.configuration.Configuration;
//import org.apache.flink.api.common.functions.RichFilterFunction;

/**
 *
 * @author cthermolia
 *
 * TopologyMetricFilter , filters service endpoint and exclude the ones that do
 * not appear in topology and metric profile data inputs
 */
public class TopologyMetricFilter extends RichFilterFunction<MetricData> {

    private transient HashMap<String, String> groupEndpoints;
    private transient HashMap<String, ArrayList<String>> metricProfileData;

    @Override
    public void open(Configuration config) throws Exception {
        super.open(config);
        ExecutionConfig configure=getRuntimeContext().getExecutionConfig();
        
        ExecutionConfig.GlobalJobParameters globalParams = configure.getGlobalJobParameters();
        Configuration globConf = (Configuration) globalParams;
        String groupEndpointsPath = globConf.getString("groupEndpointsPath",null);
        String metricDataPath = globConf.getString("metricDataPath",null);

        groupEndpoints = Utils.readGroupEndpointJson(groupEndpointsPath); //contains the information of the (group, service) matches
        metricProfileData = Utils.readMetricDataJson(metricDataPath); //contains the information of the (service, metrics) matches
    }

    @Override
    public boolean filter(MetricData t) throws Exception {
        String group = groupEndpoints.get(t.getHostname().toString() + "-") + t.getService().toString(); //retrieve the group for the service, as contained in file group_endpoints. if group is null exit 
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
