package trends.flipflops;

import trends.calculations.MetricTrends;
import trends.calculations.Trends;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import profilesmanager.MetricTagsManager;

public class MapMetricTrends extends RichMapFunction<MetricTrends, Trends> {

    List<String> mtags;
    private MetricTagsManager mtagsMgr;

    public MapMetricTrends() {

    }

    @Override
    public void open(Configuration parameters) throws IOException {
        // Initialize MetricTags manager    

        this.mtags = getRuntimeContext().getBroadcastVariable("mtags");
        this.mtagsMgr = new MetricTagsManager();
        this.mtagsMgr.loadJsonString(mtags);

    }

    @Override
    public Trends map(MetricTrends in) throws Exception {
        ArrayList<String> tags = this.mtagsMgr.getTags(in.getMetric());
        String tagInfo = "";
        for (String tag : tags) {
            if (tags.indexOf(tag) == 0) {
                tagInfo = tagInfo + tag;
            } else {
                tagInfo = tagInfo + "," + tag;
            }
        }
        return new Trends(in.getGroup(), in.getService(), in.getEndpoint(), in.getMetric(), in.getFlipflops(), tagInfo);
    }
}
