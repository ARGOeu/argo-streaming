package argo.functions.calctimelines;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import argo.pojos.EndpointTrends;
import argo.profiles.AggregationProfileParser;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cthermolia
 *
 * StatusFilter, filters data by status
 */
public class ServiceFilter implements FilterFunction<EndpointTrends> {

    static Logger LOG = LoggerFactory.getLogger(ServiceFilter.class);
    private AggregationProfileParser aggregationProfileParser;
    public ServiceFilter(AggregationProfileParser aggregationProfileParser) {
        this.aggregationProfileParser = aggregationProfileParser;
    }

    //if the status field value in Tuple equals the given status returns true, else returns false
    @Override
    public boolean filter(EndpointTrends t) throws Exception {
        ArrayList<String> services = new ArrayList<>(aggregationProfileParser.getServiceOperations().keySet());
        if (services.contains(t.getService())) {
            return true;
        }
        return false;
    }

}
