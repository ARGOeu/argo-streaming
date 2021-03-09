/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.functions.servicetrends;

import argo.avro.MetricData;
import argo.pojos.ServiceEndpTimelinePojo;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple6;

/**
 *
 * @author cthermolia
 */
public class FilterGroupTopology implements FilterFunction<ServiceEndpTimelinePojo> {

    private ArrayList<String> topologyGroups;

    public FilterGroupTopology(ArrayList<String> topologyGroups) {
        this.topologyGroups = topologyGroups;
    }

    @Override
    public boolean filter(ServiceEndpTimelinePojo t) throws Exception {

        if (topologyGroups.contains(t.getGroup())) {
            return true;
        }
        return false;

    }

}
