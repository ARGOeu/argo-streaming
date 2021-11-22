package flipflops;

import org.apache.flink.api.common.functions.MapFunction;

public class MapMetricTrends implements MapFunction<MetricTrends, Trends> {

    public MapMetricTrends() {

    }

    @Override
    public Trends map(MetricTrends in) throws Exception {
        return new Trends(in.getGroup(), in.getService(), in.getEndpoint(), in.getMetric(), in.getFlipflops(), in.getTags());
    }
}
