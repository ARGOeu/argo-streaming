package argo.batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import profilesmanager.MetricTagsManager;

/**
 * MapTupleTags produces Tuples enriched with the defined tags that correspond to the metric
 */
public class MapTupleTags extends RichFlatMapFunction<Tuple8< String, String, String, String, String, Integer, Integer, String> ,Tuple8< String, String, String, String, String, Integer, Integer, String> > {

    public MapTupleTags() {

    }

    List<String> mtags;
    private MetricTagsManager mtagsMgr;

    @Override
    public void open(Configuration parameters) throws IOException {
              // Initialize MetricTags manager    
        this.mtags = getRuntimeContext().getBroadcastVariable("mtags");

        this.mtagsMgr = new MetricTagsManager();
        this.mtagsMgr.loadJsonString(mtags);
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
    public void flatMap(Tuple8< String, String, String, String, String, Integer, Integer, String> t, Collector< Tuple8< String, String, String, String, String, Integer, Integer, String>> out) throws Exception {
        ArrayList<String> tags = this.mtagsMgr.getTags(t.f3);
        String tagInfo = "";
        for (String tag : tags) {
            if (tags.indexOf(tag) == 0) {
                tagInfo = tagInfo + tag;
            } else {
                tagInfo = tagInfo + "," + tag;
            }
        }
        Tuple8< String, String, String, String, String, Integer, Integer, String> newT = t;
        newT.f7 = tagInfo;
        out.collect(newT);

    }

}
