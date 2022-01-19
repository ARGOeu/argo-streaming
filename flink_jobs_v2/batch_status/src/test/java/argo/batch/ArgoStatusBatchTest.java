package argo.batch;

import argo.amr.ApiResource;
import argo.amr.ApiResourceManager;
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
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import profilesmanager.ReportManager;

/**
 *
 * ArgoStatusBatchTest tests the ArgoStatusBatch implementation comparing for
 * each step of calculations the generated datasets and compares them with input
 * expected datasets
 */
public class ArgoStatusBatchTest {

    public ArgoStatusBatchTest() {
    }

    /**
     * Test of main method, of class ArgoStatusBatch.
     */
    @Test
    public void testMain() throws Exception {
        System.out.println("main");
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        System.setProperty("run.date", "2022-01-14");
        final ParameterTool params = ParameterTool.fromSystemProperties();

        checkExecution(params);

        ApiResourceManager amr = mockAmr();

        DataSource<String> apsDS = env.fromElements(amr.getResourceJSON(ApiResource.AGGREGATION));
        DataSource<String> opsDS = env.fromElements(amr.getResourceJSON(ApiResource.OPS));
        DataSource<String> cfgDS = env.fromElements(amr.getResourceJSON(ApiResource.CONFIG));

        DataSource<String> recDS = env.fromElements("");
        if (amr.getResourceJSON(ApiResource.RECOMPUTATIONS) != null) {
            recDS = env.fromElements(amr.getResourceJSON(ApiResource.RECOMPUTATIONS));
        }

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
        DataSet<MetricProfile> mpsDS = env.fromElements(amr.getListMetrics());
        DataSet<GroupEndpoint> egpDS = env.fromElements(amr.getListGroupEndpoints());
        DataSet<GroupGroup> ggpDS = env.fromElements(new GroupGroup());

        GroupGroup[] listGroups = amr.getListGroupGroups();
        if (listGroups.length > 0) {
            ggpDS = env.fromElements(amr.getListGroupGroups());
        }

        Downtime[] listDowntimes = amr.getListDowntimes();
        if (listDowntimes.length > 0) {
            downDS = env.fromElements(amr.getListDowntimes());
        }

        URL mdataURL = ArgoStatusBatchTest.class.getResource("/test/mdata.avro");

        Path in = new Path(mdataURL.getPath());
        AvroInputFormat<MetricData> mdataAvro = new AvroInputFormat<MetricData>(in, MetricData.class);
        DataSet<MetricData> mdataDS = env.createInput(mdataAvro);

        URL pdataURL = ArgoStatusBatchTest.class.getResource("/test/pdata.avro");

        Path pin = new Path(pdataURL.getPath());
        AvroInputFormat<MetricData> pdataAvro = new AvroInputFormat<MetricData>(pin, MetricData.class);
        DataSet<MetricData> pdataDS = env.createInput(pdataAvro);

        DataSet<MetricData> pdataCleanDS = pdataDS.flatMap(new ExcludeMetricData()).withBroadcastSet(recDS, "rec");
        URL exppdataURL = ArgoStatusBatchTest.class.getResource("/test/exppdata.avro");

        Path exppin = new Path(exppdataURL.getPath());
        AvroInputFormat<MetricData> exppdataAvro = new AvroInputFormat<MetricData>(exppin, MetricData.class);
        DataSet<MetricData> exppdataDS = env.createInput(exppdataAvro);

        List<MetricData> expResult = exppdataDS.collect();
        List<MetricData> result = pdataCleanDS.collect();

        Assert.assertEquals(expResult, result);

        //**** Test calculation of last record of  previous data
        DataSet<MetricData> pdataMin = pdataCleanDS.groupBy("service", "hostname", "metric")
                .sortGroup("timestamp", Order.DESCENDING).first(1);

        URL lpdataURL = ArgoStatusBatchTest.class.getResource("/test/lpdata.avro");

        Path lpin = new Path(lpdataURL.getPath());
        AvroInputFormat<MetricData> lpdataAvro = new AvroInputFormat<MetricData>(lpin, MetricData.class);

        DataSet<MetricData> lpdataDS = env.createInput(lpdataAvro);

        List<MetricData> explpdata = lpdataDS.collect();
        List<MetricData> resultlpdata = pdataMin.collect();

        Assert.assertTrue(explpdata.containsAll(resultlpdata));

        //************* Test unioned metric data of previous and current date ************
        DataSet<MetricData> mdataPrevTotalDS = mdataDS.union(pdataMin);
        URL unionpdataURL = ArgoStatusBatchTest.class.getResource("/test/uniondata.avro");

        Path unionpin = new Path(unionpdataURL.getPath());
        AvroInputFormat<MetricData> unionpdataAvro = new AvroInputFormat<MetricData>(unionpin, MetricData.class);

        DataSet<MetricData> unionpdataDS = env.createInput(unionpdataAvro);

        Assert.assertTrue(unionpdataDS.collect().containsAll(mdataPrevTotalDS.collect()));

        mdataPrevTotalDS.output(new DiscardingOutputFormat<MetricData>());

        env.execute();
    }

    public List<StatusMetric> prepareInputData(List<StatusMetric> input) {

        for (StatusMetric sm : input) {
            sm.setActualData(null);
            sm.setFunction("");
            sm.setHasThr(false);
            sm.setInfo("");
            sm.setMessage(null);
            sm.setOgStatus("");
            sm.setPrevState("");
            sm.setRuleApplied("");
            sm.setSummary(null);
            sm.setTags("");
            String timestamp2 = sm.getTimestamp().split("Z")[0];
            String[] tsToken = timestamp2.split("T");
            int dateInt = Integer.parseInt(tsToken[0].replace("-", ""));
            int timeInt = Integer.parseInt(tsToken[1].replace(":", ""));
            sm.setDateInt(dateInt);
            sm.setTimeInt(timeInt);
        }
        return input;
    }

    public static String loadResJSON(String resURL) {

        InputStream jsonInputStream = ArgoStatusBatchTest.class.getResourceAsStream(resURL);
        String content = new BufferedReader(new InputStreamReader(jsonInputStream, StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n"));
        return content;

    }

    public ApiResourceManager mockAmr() {

        ApiResourceManager amr = new ApiResourceManager("localhost:8443", "s3cr3t");

        String aggrData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/aggregations.json"), false);
        String opsData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/operations.json"), false);
        String configData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/config.json"), false);
        String mprofileData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/metric_profiles.json"), false);
        String egpData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/topology_endpoints.json"), true);
        String ggpData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/topology_groups.json"), true);
        String downtimeData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/downtimes.json"), false);
        String recompData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/recomp.json"), true);

        amr.getData().put(ApiResource.AGGREGATION, aggrData);
        amr.getData().put(ApiResource.OPS, opsData);
        amr.getData().put(ApiResource.CONFIG, configData);
        amr.getData().put(ApiResource.METRIC, mprofileData);
        amr.getData().put(ApiResource.TOPOENDPOINTS, egpData);
        amr.getData().put(ApiResource.TOPOGROUPS, ggpData);
        amr.getData().put(ApiResource.DOWNTIMES, downtimeData);
        amr.getData().put(ApiResource.RECOMPUTATIONS, recompData);

        return amr;
    }

    public void checkExecution(ParameterTool params) {

        boolean calcStatus = true;
        if (params.has("calcStatus")) {
            if (params.get("calcStatus").equals("OFF")) {
                calcStatus = false;

            }
        }

        boolean calcAR = true;
        if (params.has("calcAR")) {
            if (params.get("calcAR").equals("OFF")) {
                calcAR = false;
            }
        }

        boolean calcStatusTrends = true;
        if (params.has("calcStatusTrends")) {
            if (params.get("calcStatusTrends").equals("OFF")) {
                calcStatusTrends = false;
            }
        }
        boolean calcFlipFlops = true;
        if (params.has("calcFlipFlops")) {
            if (params.get("calcFlipFlops").equals("OFF")) {
                calcFlipFlops = false;
            }
        }
        if (!calcStatus && !calcAR && !calcStatusTrends && !calcFlipFlops && !calcStatusTrends) {
            System.exit(0);
        }

    }
}
