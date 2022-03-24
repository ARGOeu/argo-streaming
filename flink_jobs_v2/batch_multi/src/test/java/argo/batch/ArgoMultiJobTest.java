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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import profilesmanager.ReportManager;

/**
 *
 * * ArgoMultiJobTest tests the ArgoStatusBatch implementation comparing for
 each step of calculations the generated datasets and compares them with input
 expected datasets
 */
public class ArgoMultiJobTest {

    public ArgoMultiJobTest() {
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

        URL mdataURL = ArgoMultiJobTest.class.getResource("/test/mdata.avro");

        Path in = new Path(mdataURL.getPath());
        AvroInputFormat<MetricData> mdataAvro = new AvroInputFormat<MetricData>(in, MetricData.class);
        DataSet<MetricData> mdataDS = env.createInput(mdataAvro);

        URL pdataURL = ArgoMultiJobTest.class.getResource("/test/pdata.avro");

        Path pin = new Path(pdataURL.getPath());
        AvroInputFormat<MetricData> pdataAvro = new AvroInputFormat<MetricData>(pin, MetricData.class);
        DataSet<MetricData> pdataDS = env.createInput(pdataAvro);

        DataSet<MetricData> pdataCleanDS = pdataDS.flatMap(new ExcludeMetricData()).withBroadcastSet(recDS, "rec");
        URL exppdataURL = ArgoMultiJobTest.class.getResource("/test/exppdata.avro");

        Path exppin = new Path(exppdataURL.getPath());
        AvroInputFormat<MetricData> exppdataAvro = new AvroInputFormat<MetricData>(exppin, MetricData.class);
        DataSet<MetricData> exppdataDS = env.createInput(exppdataAvro);

        Assert.assertEquals(exppdataDS.collect(), pdataCleanDS.collect());

        //**** Test calculation of last record of  previous data
        DataSet<MetricData> pdataMin = pdataCleanDS.groupBy("service", "hostname", "metric")
                .sortGroup("timestamp", Order.DESCENDING).first(1);

        URL lpdataURL = ArgoMultiJobTest.class.getResource("/test/lpdata.avro");

        Path lpin = new Path(lpdataURL.getPath());
        AvroInputFormat<MetricData> lpdataAvro = new AvroInputFormat(lpin, MetricData.class);

        DataSet<MetricData> lpdataDS = env.createInput(lpdataAvro);
        Assert.assertTrue(lpdataDS.collect().containsAll(pdataMin.collect()));

        //************* Test unioned metric data of previous and current date ************
        DataSet<MetricData> mdataPrevTotalDS = mdataDS.union(pdataMin);
        URL unionpdataURL = ArgoMultiJobTest.class.getResource("/test/uniondata.avro");
        Path unionpin = new Path(unionpdataURL.getPath());
        AvroInputFormat<MetricData> unionpdataAvro = new AvroInputFormat(unionpin, MetricData.class);
        DataSet<MetricData> unionpdataDS = env.createInput(unionpdataAvro);

        Assert.assertTrue(unionpdataDS.collect().containsAll(mdataPrevTotalDS.collect()));

        //***************************Test FillMIssing
        DataSet<StatusMetric> fillMissDS = mdataPrevTotalDS.reduceGroup(new FillMissing(params))
                .withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
                .withBroadcastSet(opsDS, "ops").withBroadcastSet(cfgDS, "conf");

        URL expFillMissdataURL = ArgoMultiJobTest.class.getResource("/test/fillmissing.json");
        DataSet<String> fillMissString = env.readTextFile(expFillMissdataURL.toString());
        List<String> fillMissingList = fillMissString.collect();
        List<StatusMetric> expFillMissRes = new ArrayList();
        if (!fillMissingList.isEmpty()) {
            DataSet<StatusMetric> expFillMissDS = env.fromElements(getListStatusMetric(fillMissingList.get(0)));
            expFillMissRes = prepareInputData(expFillMissDS.collect());
        }

        Assert.assertTrue(expFillMissRes.containsAll(fillMissDS.collect()));

        //**************** Test PickEndpoints
        DataSet<StatusMetric> mdataTrimDS = mdataPrevTotalDS.flatMap(new PickEndpoints(params))
                .withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
                .withBroadcastSet(recDS, "rec").withBroadcastSet(cfgDS, "conf").withBroadcastSet(thrDS, "thr")
                .withBroadcastSet(opsDS, "ops").withBroadcastSet(apsDS, "aps");

        URL expPickdataURL = ArgoMultiJobTest.class.getResource("/test/pickdata.json");
        DataSet<String> expPickDataString = env.readTextFile(expPickdataURL.toString());
        List<String> pickDataList = expPickDataString.collect();
        List<StatusMetric> expPickDataRes = new ArrayList();
        if (!pickDataList.isEmpty()) {
            DataSet<StatusMetric> expPickDataDS = env.fromElements(getListStatusMetric(pickDataList.get(0)));
            expPickDataRes = preparePickData(expPickDataDS.collect());
        }
//        List<MetricData> result = mdataPrevTotalDS.collect();

        Assert.assertTrue(expPickDataRes.containsAll(mdataTrimDS.collect()));

        DataSet<StatusMetric> mdataTotalDS = mdataTrimDS.union(fillMissDS);
        Assert.assertTrue(expPickDataRes.containsAll(mdataTotalDS.collect()));

        //********************* Test Map Services
        mdataTotalDS = mdataTotalDS.flatMap(new MapServices()).withBroadcastSet(apsDS, "aps");

        URL mapServicesURL = ArgoMultiJobTest.class.getResource("/test/mapservices.json");
        DataSet<String> mapServicesString = env.readTextFile(mapServicesURL.toString());
        List<String> mapServicesList = mapServicesString.collect();
        List<StatusMetric> expMapServicesRes = new ArrayList();
        if (!mapServicesList.isEmpty()) {
            DataSet<StatusMetric> expMapServicesDS = env.fromElements(getListStatusMetric(mapServicesList.get(0)));
            expMapServicesRes = preparePickData(expMapServicesDS.collect());
        }

        Assert.assertTrue(expMapServicesRes.containsAll(mdataTotalDS.collect()));

        //************** Test CalcPrevStatus
        DataSet<StatusMetric> stDetailDS = mdataTotalDS.groupBy("group", "service", "hostname", "metric")
                .sortGroup("timestamp", Order.ASCENDING).reduceGroup(new CalcPrevStatus(params))
                .withBroadcastSet(mpsDS, "mps").withBroadcastSet(recDS, "rec").withBroadcastSet(opsDS, "ops");

        URL calcPrevStatusURL = ArgoMultiJob.class.getResource("/test/expCalcPrevStatus.json");
        DataSet<String> calcPrevStatusString = env.readTextFile(calcPrevStatusURL.toString());
        List<String> calcPrevStatusList = calcPrevStatusString.collect();
        List<StatusMetric> expCalcPrevStatusRes = new ArrayList();
        if (!calcPrevStatusList.isEmpty()) {
            DataSet<StatusMetric> expCalcPrepStatusDS = env.fromElements(getListCalcPrevData(calcPrevStatusList.get(0)));
            expCalcPrevStatusRes = preparePickData(expCalcPrepStatusDS.collect());
        }

        Assert.assertTrue(expCalcPrevStatusRes.containsAll(stDetailDS.collect()));
   
        mdataPrevTotalDS.output(new DiscardingOutputFormat<MetricData>());
        env.execute();

    }

    public List<StatusMetric> prepareInputData(List<StatusMetric> input) {

        for (StatusMetric sm : input) {
            sm.setActualData("");
            sm.setFunction("");
            sm.setHasThr(false);
            sm.setInfo("");
            sm.setMessage("");
            sm.setOgStatus("");
            sm.setPrevState("");
            sm.setRuleApplied("");
            sm.setSummary("");
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

    public List<StatusMetric> preparePickData(List<StatusMetric> input) {

        for (StatusMetric sm : input) {
            sm.setActualData(null);
            sm.setHasThr(false);
            sm.setInfo("");
            sm.setMessage(null);
            sm.setOgStatus("");
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

        InputStream jsonInputStream = ArgoMultiJobTest.class.getResourceAsStream(resURL);
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

    /**
     * Parses the Metric profile content retrieved from argo-web-api and
     * provides a list of MetricProfile avro objects to be used in the next
     * steps of the pipeline
     */
    public StatusMetric[] getListStatusMetric(String content) {
        List<StatusMetric> results = new ArrayList<StatusMetric>();

        JsonParser jsonParser = new JsonParser();
        JsonElement jElement = jsonParser.parse(content);
        JsonArray jRoot = jElement.getAsJsonArray();
        for (int i = 0; i < jRoot.size(); i++) {
            JsonObject jItem = jRoot.get(i).getAsJsonObject();

            String group = jItem.get("group").getAsString();
            String service = jItem.get("service").getAsString();
            String hostname = jItem.get("hostname").getAsString();
            String function = "";
            if (jItem.has("function")) {
                function = jItem.get("function").getAsString();
            }
            String metric = jItem.get("metric").getAsString();
            String status = jItem.get("status").getAsString();
            String timestamp = jItem.get("timestamp").getAsString();
            StatusMetric stm = new StatusMetric();
            stm.setGroup(group);
            stm.setService(service);
            stm.setHostname(hostname);
            stm.setMetric(metric);
            stm.setStatus(status);
            stm.setTimestamp(timestamp);
            stm.setFunction(function);
            results.add(stm);
        }
        StatusMetric[] rArr = new StatusMetric[results.size()];
        rArr = results.toArray(rArr);
        return rArr;
    }

    /**
     * Parses the Metric profile content retrieved from argo-web-api and
     * provides a list of MetricProfile avro objects to be used in the next
     * steps of the pipeline
     */
    public StatusMetric[] getListCalcPrevData(String content) {
        List<StatusMetric> results = new ArrayList<StatusMetric>();

        JsonParser jsonParser = new JsonParser();
        JsonElement jElement = jsonParser.parse(content);
        JsonArray jRoot = jElement.getAsJsonArray();
        for (int i = 0; i < jRoot.size(); i++) {
            JsonObject jItem = jRoot.get(i).getAsJsonObject();

            String group = jItem.get("group").getAsString();
            String service = jItem.get("service").getAsString();
            String hostname = jItem.get("hostname").getAsString();
            String function = "";
            String prevTs = "";
            String prevState = "";

            if (jItem.has("function")) {
                function = jItem.get("function").getAsString();
            }
            if (jItem.has("prevState")) {
                prevState = jItem.get("prevState").getAsString();
            }
            if (jItem.has("prevTs")) {
                prevTs = jItem.get("prevTs").getAsString();
            }
            String metric = jItem.get("metric").getAsString();
            String status = jItem.get("status").getAsString();
            String timestamp = jItem.get("timestamp").getAsString();
            StatusMetric stm = new StatusMetric();
            stm.setGroup(group);
            stm.setService(service);
            stm.setHostname(hostname);
            stm.setMetric(metric);
            stm.setStatus(status);
            stm.setTimestamp(timestamp);
            stm.setFunction(function);
            stm.setPrevState(prevState);
            stm.setPrevTs(prevTs);
            results.add(stm);
        }
        StatusMetric[] rArr = new StatusMetric[results.size()];
        rArr = results.toArray(rArr);
        return rArr;
    }
}