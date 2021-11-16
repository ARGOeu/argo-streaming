package argo.batch;

import java.io.IOException;
import java.util.List;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import profilesmanager.AggregationProfileManager;

/**
 * MapServices produces TimelineTrends for each service,that maps to the groups
 * of functions as described in aggregation profile groups endpoint , metric
 */
public class MapServices extends RichFlatMapFunction<StatusMetric, StatusMetric> {

    public MapServices() {

    }

    private List<String> aps;
    private AggregationProfileManager apsMgr;

    @Override
    public void open(Configuration parameters) throws IOException {

        // Get data from broadcast variables
        this.aps = getRuntimeContext().getBroadcastVariable("aps");

        // Initialize aggregation profile manager
        this.apsMgr = new AggregationProfileManager();
        this.apsMgr.loadJsonString(aps);
        // Initialize operations manager    

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
        String service = t.getService();

        String function = this.apsMgr.retrieveFunctionsByService(service);
        StatusMetric newT = t;
        newT.setFunction(function);
        out.collect(newT);

    }

}
