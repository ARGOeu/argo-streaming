package trends.flipflops;

import trends.calculations.EndpointTrends;
import trends.calculations.Trends;
import org.apache.flink.api.common.functions.MapFunction;

public class MapEndpointTrends implements MapFunction<EndpointTrends, Trends> {

    public MapEndpointTrends() {

    }

    @Override
    public Trends map(EndpointTrends in) throws Exception {
        return new Trends(in.getGroup(), in.getService(), in.getEndpoint(), in.getFlipflops());
    }
}
