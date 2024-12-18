package argo.batch;

import argo.amr.ApiResource;
import argo.amr.ApiResourceManager;
import argo.avro.MetricProfile;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Assert;
import org.junit.Test;
import profilesmanager.ReportManager;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CalcPrevStatusDataTest {

    @Test
    public void testMain() throws Exception {
        System.out.println("main");
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);
        System.setProperty("run.date", "2025-02-01");
        //we test if for a period extending today all the today data has EXCLUDED status
        // exclude period:  "start_time": "2025-02-01T00:00:00Z" - "end_time": "2025-02-02T00:00:00Z",
        //the exclude period extends today , so  all expected data should have EXCLUDED status. note tha
        //the first elements should keep it's previous status MISSING as no previous data exist in dataset

        testRecomputationData(env, "/test/recomputations/recomp_all.json", "/test/recomputations/initialMetricData.json", "/test/recomputations/expRecomputationData_1.json");

        System.setProperty("run.date", "2025-02-02");
        //we test if for a period extending today all the today data has EXCLUDED status and previous state is correctly EXCLUDED as period extends today
        //the initial dataset has also yesterday data to check if previous status is correctly calculated
        // exclude period:  "start_time": "2025-02-01T00:00:00Z" - "end_time": "2025-02-02T00:00:00Z",
        //the exclude period extends today , so  all expected data should have EXCLUDED status. note tha
        //the first elements should have is previous status EXCLUDED as the exclude period includes yesterday

        testRecomputationData(env, "/test/recomputations/recomp_all.json", "/test/recomputations/initialMetricData.json", "/test/recomputations/expRecomputationData_2.json");
        //we test the case the period starts at today and ends inside today , so the data inside the period are
        //correctly excluded and the data after the period are correctly keep their status.
        // exclude period: "start_time": "2025-02-01T00:00:00Z"- "end_time": "2025-02-02T04:00:00Z",
        //
        //the exclude period  starts at the today and ends inside today , so  all data  before the end of the exclude period should have EXCLUDED status.
        // and all the data after the end of exclude period should keep their status.
        //as no data  exists with timestamp exactly the end of the exclude period we need to
        // add one data element to define that the regular period starts , with status
        // the  one of the element before had initially

        testRecomputationData(env, "/test/recomputations/recomp_all_3.json", "/test/recomputations/initialMetricData_3.json", "/test/recomputations/expRecomputationData_3.json");
        //we test the case the  period starts and ends inside  today
        // so the data inside the period should be correctly excluded and the data before and after the period  correctly keep their status.
        // exclude period:    "start_time": "2025-02-02T02:30:00Z" - "end_time": "2025-02-02T04:30:00Z",

        //as no data  exists with timestamp exactly at the start of the exclude period we need to
        // add one data element to define that the exclude period starts.

        //as no data  exists with timestamp exactly the end of the exclude period we need to
        // add one data element to define that the regular period starts , with status
        // the  one of the element before had initially

        testRecomputationData(env, "/test/recomputations/recomp_all_4.json", "/test/recomputations/initialMetricData_4.json", "/test/recomputations/expRecomputationData_4.json");
        //we test the case that no recomputation exists, data should have their initial statuses
        testRecomputationData(env, "/test/recomputations/recomp_all_6.json", "/test/recomputations/initialMetricData_4.json", "/test/recomputations/expRecomputationData_6.json");
        // //we test the case the  period starts before today and ends inside  today
        // so  all data  before the end of the exclude period should have EXCLUDED status.
        // exclude period:         "start_time": "2025-02-01T00:00:00Z" - "end_time": "2025-02-02T02:00:00Z",
        // and all the data after the end of exclude period should keep their status.
        //as no data  exists with timestamp exactly the end of the exclude period we need to
        // add one data element to define that the regular period starts , with status
        // the  one of the element before had initially

        testRecomputationData(env, "/test/recomputations/recomp_all_7.json", "/test/recomputations/initialMetricData_5.json", "/test/recomputations/expRecomputationData_7.json");
        // //we test the case the  period starts inside today and ends tomorrow
        // so  all data  after the period starts should have EXCLUDED status.
        // exclude period:   "start_time": "2025-02-02T21:00:00Z" -     "end_time": "2025-02-03T00:00:00Z",

        // as no data  exists with timestamp exactly the start of the exclude period add one data element to define that the regular period starts , with status

        testRecomputationData(env, "/test/recomputations/recomp_all_8.json", "/test/recomputations/initialMetricData_4.json", "/test/recomputations/expRecomputationData_8.json");

        env.execute();
    }

    public static void testRecomputationData(ExecutionEnvironment env, String recomputationDataset, String initialDataset, String expectedDataset) throws Exception {
        final ParameterTool params = ParameterTool.fromSystemProperties();

        ApiResourceManager amr = mockAmr(recomputationDataset);

        DataSource<String> cfgDS = env.fromElements(amr.getResourceJSON(ApiResource.CONFIG));

        DataSource<String> confDS = env.fromElements(amr.getResourceJSON(ApiResource.CONFIG));
        // Get conf data
        List<String> confData = confDS.collect();
        ReportManager cfgMgr = new ReportManager();
        cfgMgr.loadJsonString(confData);

        DataSource<String> opsDS = env.fromElements(amr.getResourceJSON(ApiResource.OPS));

        DataSource<String> recDS = env.fromElements("");
        if (amr.getResourceJSON(ApiResource.RECOMPUTATIONS) != null) {
            recDS = env.fromElements(amr.getResourceJSON(ApiResource.RECOMPUTATIONS));
        }

        ReportManager confMgr = new ReportManager();
        confMgr.loadJsonString(cfgDS.collect());

        DataSet<MetricProfile> mpsDS = env.fromElements(amr.getListMetrics());

        URL initialDataURL = ArgoMultiJobTest.class.getResource(initialDataset);
        DataSet<String> initialMetricDataString = env.readTextFile(initialDataURL.toString());
        List<String> initialMetricDataList = initialMetricDataString.collect();
        List<StatusMetric> initialMetricData = new ArrayList();
        if (!initialMetricDataList.isEmpty()) {
            DataSet<StatusMetric> expMapServicesDS = env.fromElements(getListStatusMetric(initialMetricDataList.get(0)));
            initialMetricData = preparePickData(expMapServicesDS.collect());
        }
        DataSet<StatusMetric> initialMetricDataDS = env.fromCollection(initialMetricData);

        DataSet<StatusMetric> calcRecompDS = initialMetricDataDS.distinct("group", "service", "hostname", "metric", "status", "timestamp").groupBy("group", "service", "hostname", "metric")
                .sortGroup("timestamp", Order.ASCENDING).reduceGroup(new CalcRecomputation(params))
                .withBroadcastSet(mpsDS, "mps").withBroadcastSet(recDS, "rec").withBroadcastSet(opsDS, "ops");

        DataSet<StatusMetric> stDetailDS = calcRecompDS.groupBy("group", "service", "hostname", "metric")
                .sortGroup("timestamp", Order.ASCENDING).reduceGroup(new CalcPrevStatus(params))
                .withBroadcastSet(mpsDS, "mps").withBroadcastSet(recDS, "rec").withBroadcastSet(opsDS, "ops");
        for (StatusMetric res : stDetailDS.collect()) {
            System.out.println("res-- " + res.toString());
        }
        URL calcPrevStatusURL = ArgoMultiJob.class.getResource(expectedDataset);
        DataSet<String> calcPrevStatusString = env.readTextFile(calcPrevStatusURL.toString());
        List<String> calcPrevStatusList = calcPrevStatusString.collect();
        List<StatusMetric> expCalcPrevStatusRes = new ArrayList();
        if (!calcPrevStatusList.isEmpty()) {
            DataSet<StatusMetric> expCalcPrepStatusDS = env.fromElements(getListCalcPrevData(calcPrevStatusList.get(0)));
            expCalcPrevStatusRes = preparePickData(expCalcPrepStatusDS.collect());
        }
        for (StatusMetric exp : expCalcPrevStatusRes) {
            System.out.println("exp-- " + exp);
        }
        Assert.assertEquals(TestUtils.compareLists(expCalcPrevStatusRes, stDetailDS.collect()), true);
        stDetailDS.output(new DiscardingOutputFormat<StatusMetric>());

    }

    public static StatusMetric[] getListStatusMetric(String content) {
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
            String ruleApplied = "";
            String ogstatus = "";
            String actualdata = "";

            if (jItem.has("function")) {
                function = jItem.get("function").getAsString();
            }
            if (jItem.has("ogstatus")) {
                ogstatus = jItem.get("ogstatus").getAsString();

            }
            if (jItem.has("rule_applied")) {
                ruleApplied = jItem.get("rule_applied").getAsString();

            }
            if (jItem.has("actual_data")) {
                actualdata = jItem.get("actual_data").getAsString();

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
            stm.setRuleApplied(ruleApplied);
            stm.setOgStatus(ogstatus);
            stm.setActualData(actualdata);
            results.add(stm);
        }
        StatusMetric[] rArr = new StatusMetric[results.size()];
        rArr = results.toArray(rArr);
        return rArr;
    }

    public static List<StatusMetric> preparePickData(List<StatusMetric> input) {

        for (StatusMetric sm : input) {

            if (sm.getActualData().equals("")) {
                sm.setActualData(null);
            }

            sm.setInfo("{}");
            sm.setMessage(null);
            if (sm.getOgStatus() == null) {
                sm.setOgStatus("");
            }
            if (sm.getRuleApplied() == null) {
                sm.setRuleApplied("");
            }
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

    public static ApiResourceManager mockAmr(String recomputationDataset) {

        ApiResourceManager amr = new ApiResourceManager("localhost:8443", "s3cr3t");

        String aggrData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/aggregations.json"), false);
        String opsData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/operations.json"), false);
        String configData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/config.json"), false);
        String mprofileData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/metric_profiles.json"), false);
        String egpData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/topology_endpoints.json"), true);
        String ggpData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/topology_groups.json"), true);
        String downtimeData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/downtimes.json"), false);
        String recompData = amr.getApiResponseParser().getJsonData(loadResJSON(recomputationDataset), true);
        String mtagsData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/metrics.json"), true);
        String thrData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/threshold.json"), false);
        amr.getData().put(ApiResource.AGGREGATION, aggrData);
        amr.getData().put(ApiResource.OPS, opsData);
        amr.getData().put(ApiResource.CONFIG, configData);
        amr.getData().put(ApiResource.METRIC, mprofileData);
        amr.getData().put(ApiResource.TOPOENDPOINTS, egpData);
        amr.getData().put(ApiResource.TOPOGROUPS, ggpData);
        amr.getData().put(ApiResource.DOWNTIMES, downtimeData);
        amr.getData().put(ApiResource.RECOMPUTATIONS, recompData);
        amr.getData().put(ApiResource.MTAGS, mtagsData);
        amr.getData().put(ApiResource.THRESHOLDS, thrData);

        return amr;
    }

    public static String loadResJSON(String resURL) {

        InputStream jsonInputStream = ArgoMultiJobTest.class
                .getResourceAsStream(resURL);
        String content = new BufferedReader(new InputStreamReader(jsonInputStream, StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n"));
        return content;

    }

    public static StatusMetric[] getListCalcPrevData(String content) {
        List<StatusMetric> results = new ArrayList<StatusMetric>();

        JsonParser jsonParser = new JsonParser();
        JsonElement jElement = jsonParser.parse(content);
        JsonArray jRoot = jElement.getAsJsonArray();
        for (int i = 0; i < jRoot.size(); i++) {
            JsonObject jItem = jRoot.get(i).getAsJsonObject();

            String group = jItem.get("group").getAsString();
            String prevTs = "";
            String prevState = "";
            String tags = "";
            String metric = "";
            String hostname = "";
            String service = "";
            String function = "";
            String ruleApplied = "";
            String ogstatus = "";
            String actualdata = "";
            boolean hasThr = false;

            if (jItem.has("metric") && !jItem.get("metric").isJsonNull()) {
                metric = jItem.get("metric").getAsString();
            }

            if (jItem.has("hostname") && !jItem.get("hostname").isJsonNull()) {
                hostname = jItem.get("hostname").getAsString();
            }
            if (jItem.has("service") && !jItem.get("service").isJsonNull()) {
                service = jItem.get("service").getAsString();
            }

            if (jItem.has("function")) {
                function = jItem.get("function").getAsString();
            }
            if (jItem.has("prevState")) {
                prevState = jItem.get("prevState").getAsString();
            }
            if (jItem.has("prevTs")) {
                prevTs = jItem.get("prevTs").getAsString();
            }

            if (jItem.has("tags") && !jItem.get("tags").isJsonNull()) {
                tags = jItem.get("tags").getAsString();
            }
            if (jItem.has("ogstatus")) {
                ogstatus = jItem.get("ogstatus").getAsString();

            }
            if (jItem.has("rule_applied")) {
                ruleApplied = jItem.get("rule_applied").getAsString();
                hasThr = true;

            }
            if (jItem.has("actual_data")) {
                actualdata = jItem.get("actual_data").getAsString();

            }

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
            stm.setTags(tags);
            stm.setRuleApplied(ruleApplied);
            stm.setOgStatus(ogstatus);
            stm.setActualData(actualdata);
            stm.setHasThr(hasThr);
            results.add(stm);
        }
        StatusMetric[] rArr = new StatusMetric[results.size()];
        rArr = results.toArray(rArr);
        return rArr;
    }

}
