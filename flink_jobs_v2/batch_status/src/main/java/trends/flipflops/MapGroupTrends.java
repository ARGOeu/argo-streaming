package trends.flipflops;

import trends.calculations.GroupTrends;
import trends.calculations.Trends;
import org.apache.flink.api.common.functions.MapFunction;

public class MapGroupTrends implements MapFunction<GroupTrends, Trends> {

    public MapGroupTrends() {

    }

    @Override
    public Trends map(GroupTrends in) throws Exception {
        return new Trends(in.getGroup(), in.getFlipflops());
    }
}
