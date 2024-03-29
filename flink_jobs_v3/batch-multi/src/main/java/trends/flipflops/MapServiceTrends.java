package trends.flipflops;

import trends.calculations.ServiceTrends;
import trends.calculations.Trends;
import org.apache.flink.api.common.functions.MapFunction;

public class MapServiceTrends implements MapFunction<ServiceTrends, Trends> {

    public MapServiceTrends() {

    }

    @Override
    public Trends map(ServiceTrends in) throws Exception {
        return new Trends(in.getGroup(), in.getService(),  in.getFlipflops());
    }
}
