/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/UnitTests/JUnit4TestClass.java to edit this template
 */
package argo.batch;

import argo.avro.Downtime;
import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;
import argo.avro.MetricData;
import argo.avro.MetricProfile;
import argo.avro.Weight;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Assert;
import org.junit.Test;
import org.junit.ClassRule;
import profilesmanager.ReportManager;

/**
 *
 * @author cthermolia
 */
public class ArgoStatusBatchIT {

    public ArgoStatusBatchIT() {
    }
//     @ClassRule
//     public static MiniClusterWithClientResource flinkCluster =
//         new MiniClusterWithClientResource(
//             new MiniClusterResourceConfiguration.Builder()
//                 .setNumberSlotsPerTaskManager(2)
//                 .setNumberTaskManagers(1)
//                 .build());

    /**
     * Test of main method, of class ArgoStatusBatch.
     */
    @Test
    public void testMain() throws Exception {
        System.out.println("main");
        readInputData();

//   ArgoStatusBatch.main(args);
        // TODO review the generated test code and remove the default call to fail.
        //     fail("The test case is a prototype.");
    }

    public void readInputData() throws Exception {
        boolean calcStatus = true;
        boolean calcAR = true;
        boolean calcStatusTrends = true;
        boolean calcFlipFlops = true;
        String[] args = new String[2];
        args[0] = "--pdata hdfs://localhost:9000/user/cthermolia/egi_29_03_2021";
        args[1] = "--mdata hdfs://localhost:9000/user/cthermolia/egi_30_03_2021";
        final ParameterTool params = ParameterTool.fromArgs(args);
        System.out.println("mdata is: " + params.get("mdata"));

        if (!calcStatus && !calcAR && !calcStatusTrends && !calcFlipFlops && !calcStatusTrends) {
            System.exit(0);
        }
        int rankNum = 10;
        boolean clearMongo = true;
        //  --pdata hdfs://localhost:9000/user/cthermolia/egi_29_03_2021 --mdata hdfs://localhost:9000/user/cthermolia/egi_30_03_2021 
        //--N 10 --run.date 2021-03-30    --mongo.uri  mongodb://localhost:27017/argo_results --mongo.method insert --api.timeout 30
        String apiEndpoint = "api.devel.argo.grnet.gr";
        String apiToken = "e33cd8106007e7e91729c43adb93e5db";
        String reportID = "8b917325-bc45-4d72-9f31-5e2e2ca16bbf";

        String runDate = "2022-01-14 ";

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        URL aggrFile = ArgoStatusBatchIT.class.getResource("/test/aggregations.json");
        URL opsFile = ArgoStatusBatchIT.class.getResource("/test/operations.json");
        URL configFile = ArgoStatusBatchIT.class.getResource("/test/config.json");

        //File jsonFile = new File(resJsonFile.toURI());
        DataSource<String> apsDS = env.readTextFile(aggrFile.toString());
        DataSource<String> opsDS = env.readTextFile(opsFile.toString());
        DataSource<String> cfgDS = env.readTextFile(configFile.toString());

        DataSource<String> recDS = env.fromElements("");

        DataSource<String> mtagsDS = env.fromElements("");
        DataSet<Weight> weightDS = env.fromElements(new Weight());
        Weight[] listWeights = new Weight[0];

        DataSet<Downtime> downDS = env.fromElements(new Downtime());
        // begin with empty threshold datasource
        DataSource<String> thrDS = env.fromElements("");
        ReportManager confMgr = new ReportManager();
        confMgr.loadJsonString(cfgDS.collect());

        // Get conf data 
        List<String> confData = cfgDS.collect();
        ReportManager cfgMgr = new ReportManager();
        cfgMgr.loadJsonString(confData);

        URL mprofileFile = ArgoStatusBatchIT.class.getResource("/test/metric_profiles.json");
        DataSet<String> mpString = env.readTextFile(mprofileFile.toString());
        DataSet<MetricProfile> mpsDS = env.fromElements(getListMetrics(mpString.collect().get(0)));

        URL topEndFile = ArgoStatusBatchIT.class.getResource("/test/topology_endpoints.json");
        DataSet<String> topEndString = env.readTextFile(topEndFile.toString());
        DataSet<GroupEndpoint> egpDS = env.fromElements(getListGroupEndpoints(topEndString.collect().get(0)));

        URL topGroupFile = ArgoStatusBatchIT.class.getResource("/test/topology_groups.json");
        DataSet<String> topGroupString = env.readTextFile(topGroupFile.toString());
        DataSet<GroupGroup> ggpDS = env.fromElements(getListGroupGroups(topGroupString.collect().get(0)));

        URL downFile = ArgoStatusBatchIT.class.getResource("/test/downtimes.json");
        DataSet<String> downString = env.readTextFile(downFile.toString());
        downDS = env.fromElements(getListDowntimes(downString.collect().get(0)));

        // todays metric data
        URL mdataURL = ArgoStatusBatchIT.class.getResource("/test/mdata.avro");

        Path in = new Path(mdataURL.getPath());
        System.out.println("is a path ---- "+in);
        AvroInputFormat<MetricData> mdataAvro = new AvroInputFormat<MetricData>(in, MetricData.class);
        DataSet<MetricData> mdataDS = env.createInput(mdataAvro);

        URL pdataURL = ArgoStatusBatchIT.class.getResource("/test/pdata.avro");

        Path pin = new Path(pdataURL.getPath());
        AvroInputFormat<MetricData> pdataAvro = new AvroInputFormat<MetricData>(pin, MetricData.class);
        DataSet<MetricData> pdataDS = env.createInput(pdataAvro);
        DataSet<MetricData> testDt = pdataDS;

        DataSet<MetricData> pdataCleanDS = pdataDS.flatMap(new ExcludeMetricData()).withBroadcastSet(recDS, "rec");

        Assert.assertEquals(testDt.collect(), pdataCleanDS.collect());
//        env.execute();

    }

    /**
     * Parses the Metric profile content retrieved from argo-web-api and
     * provides a list of MetricProfile avro objects to be used in the next
     * steps of the pipeline
     */
    public MetricProfile[] getListMetrics(String content) {
        List<MetricProfile> results = new ArrayList<MetricProfile>();

        JsonParser jsonParser = new JsonParser();
        JsonElement jElement = jsonParser.parse(content);
        JsonObject jRoot = jElement.getAsJsonObject();
        String profileName = jRoot.get("name").getAsString();
        JsonArray jElements = jRoot.get("services").getAsJsonArray();
        for (int i = 0; i < jElements.size(); i++) {
            JsonObject jItem = jElements.get(i).getAsJsonObject();
            String service = jItem.get("service").getAsString();
            JsonArray jMetrics = jItem.get("metrics").getAsJsonArray();
            for (int j = 0; j < jMetrics.size(); j++) {
                String metric = jMetrics.get(j).getAsString();

                Map<String, String> tags = new HashMap<String, String>();
                MetricProfile mp = new MetricProfile(profileName, service, metric, tags);
                results.add(mp);
            }

        }
        MetricProfile[] rArr = new MetricProfile[results.size()];
        rArr = results.toArray(rArr);
        return rArr;
    }

    public GroupEndpoint[] getListGroupEndpoints(String content) {
        List<GroupEndpoint> results = new ArrayList<GroupEndpoint>();

        JsonParser jsonParser = new JsonParser();
        JsonElement jElement = jsonParser.parse(content);
        JsonArray jRoot = jElement.getAsJsonArray();
        for (int i = 0; i < jRoot.size(); i++) {
            JsonObject jItem = jRoot.get(i).getAsJsonObject();
            String group = jItem.get("group").getAsString();
            String gType = jItem.get("type").getAsString();
            String service = jItem.get("service").getAsString();
            String hostname = jItem.get("hostname").getAsString();
            JsonObject jTags = jItem.get("tags").getAsJsonObject();
            Map<String, String> tags = new HashMap<String, String>();
            for (Map.Entry<String, JsonElement> kv : jTags.entrySet()) {
                tags.put(kv.getKey(), kv.getValue().getAsString());
            }
            GroupEndpoint ge = new GroupEndpoint(gType, group, service, hostname, tags);
            results.add(ge);
        }

        GroupEndpoint[] rArr = new GroupEndpoint[results.size()];
        rArr = results.toArray(rArr);
        return rArr;

    }

    /**
     * Parses the Topology Groups content retrieved from argo-web-api and
     * provides a list of GroupGroup avro objects to be used in the next steps
     * of the pipeline
     */
    public GroupGroup[] getListGroupGroups(String content) {
        List<GroupGroup> results = new ArrayList<GroupGroup>();
        JsonParser jsonParser = new JsonParser();
        JsonElement jElement = jsonParser.parse(content);
        JsonArray jRoot = jElement.getAsJsonArray();
        for (int i = 0; i < jRoot.size(); i++) {
            JsonObject jItem = jRoot.get(i).getAsJsonObject();
            String group = jItem.get("group").getAsString();
            String gType = jItem.get("type").getAsString();
            String subgroup = jItem.get("subgroup").getAsString();
            JsonObject jTags = jItem.get("tags").getAsJsonObject();
            Map<String, String> tags = new HashMap<String, String>();
            for (Map.Entry<String, JsonElement> kv : jTags.entrySet()) {
                tags.put(kv.getKey(), kv.getValue().getAsString());
            }
            GroupGroup gg = new GroupGroup(gType, group, subgroup, tags);
            results.add(gg);
        }

        GroupGroup[] rArr = new GroupGroup[results.size()];
        rArr = results.toArray(rArr);
        return rArr;
    }

    public Downtime[] getListDowntimes(String content) {
        List<Downtime> results = new ArrayList<Downtime>();
        JsonParser jsonParser = new JsonParser();
        JsonElement jElement = jsonParser.parse(content);
        JsonObject jRoot = jElement.getAsJsonObject();
        JsonArray jElements = jRoot.get("endpoints").getAsJsonArray();
        for (int i = 0; i < jElements.size(); i++) {
            JsonObject jItem = jElements.get(i).getAsJsonObject();
            String hostname = jItem.get("hostname").getAsString();
            String service = jItem.get("service").getAsString();
            String startTime = jItem.get("start_time").getAsString();
            String endTime = jItem.get("end_time").getAsString();

            Downtime d = new Downtime(hostname, service, startTime, endTime);
            results.add(d);
        }
        Downtime[] rArr = new Downtime[results.size()];
        rArr = results.toArray(rArr);
        return rArr;

    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<MetricData> {

        // must be static
        public static final List<MetricData> values = Collections.synchronizedList(new ArrayList<MetricData>());

        @Override
        public void invoke(MetricData value) throws Exception {
            values.add(value);
        }
    }

}
