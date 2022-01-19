package argo.batch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import static org.junit.Assert.assertTrue;
import org.junit.ClassRule;
import org.junit.Test;

public class ExampleIntegrationTest {
    
//    @ClassRule
//    public static MiniClusterWithClientResource flinkCluster
//            = new MiniClusterWithClientResource(
//                    new MiniClusterResourceConfiguration.Builder()
//                            .setNumberSlotsPerTaskManager(2)
//                            .setNumberTaskManagers(1)
//                            .setShutdownTimeout(Time.seconds(60))
//                            .build());
    
    @Test
    public void testIncrementPipeline() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(2);

        // values are collected in a static variable
        CollectSink.values.clear();

        // create a stream of custom elements and apply transformations
        env.fromElements(1L, 21L, 22L)
                .map(new IncrementMapFunction())
                .addSink(new CollectSink());

        // execute
        env.execute();
        List<Long> values = Collections.synchronizedList(new ArrayList<Long>());
        
        ArrayList<Long> arraylist = new ArrayList<>();
        arraylist.add(2L);
        arraylist.add(22L);
        arraylist.add(23L);
        // verify your results
        assertTrue(CollectSink.values.containsAll(arraylist));
    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<Long> {
        
        public static final List<Long> values = Collections.synchronizedList(new ArrayList<Long>());
        
        @Override
        public void invoke(Long value) throws Exception {
            values.add(value);
        }
    }
}
