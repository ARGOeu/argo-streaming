package argo.batch;

import argo.amr.ApiResource;
import argo.amr.ApiResourceManager;
import argo.avro.*;
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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;
import profilesmanager.OperationsManager;
import profilesmanager.RecomputationsManager;
import profilesmanager.ReportManager;
import utils.Utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class RecomputationChangeStatusTest {
    private static DataSource<String> cfgDS;
    private static DataSource<String> confDS;
    private static DataSource<String> apsDS;
    private static DataSource<String> opsDS;
    private static OperationsManager opsManager;
    private static DataSource<String> mtagsDS;
    private static DataSet<Weight> weightDS;
    private static DataSet<Downtime> downDS;
    private static DataSource<String> thrDS;
    private static DataSet<MetricProfile> nempsDS;
    private static DataSet<MetricProfile> mpsDS;
    private static DataSet<GroupEndpoint> egpDS;
    private static DataSet<GroupGroup> ggpDS;
    private static ArrayList<String> opsJson;
    private static ArrayList<String> aggrJson;
    private static DateTime now = new DateTime(DateTimeZone.UTC);
    private static ApiResourceManager amr;
    private static String testFilePath = "/test/recomputations";
    private static DataSource<String> recDS;

    @Test
    public void testMain() throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);
        System.setProperty("run.date", "2025-04-30");

        final ParameterTool params = ParameterTool.fromSystemProperties();

        /**********/

        DataSet<StatusTimeline> statusMetricTimeline1 = testMetricTimelines(env, params, testFilePath + "/initial_group_timeline_data_recomp.json", testFilePath + "/metric_timeline_recomp1.json", testFilePath + "/exp_metric_timeline1_metricA.json", testFilePath + "/exp_metric_recomputed.json", true);
        DataSet<StatusTimeline> statusMetricTimeline2 = testMetricTimelines(env, params, testFilePath + "/initial_group_timeline_data_recomp.json", testFilePath + "/metric_timeline_recomp2.json", testFilePath + "/exp_metric_timeline2_metricA.json", testFilePath + "/exp_metric_recomputed2.json", true);
        DataSet<StatusTimeline> statusMetricTimeline3 = testMetricTimelines(env, params, testFilePath + "/initial_group_timeline_data_recomp.json", testFilePath + "/metric_timeline_recomp3.json", testFilePath + "/exp_metric_timeline2_metricA.json", testFilePath + "/exp_metric_recomputed.json", true);
        DataSet<StatusTimeline> statusMetricTimeline4 = testMetricTimelines(env, params, testFilePath + "/initial_group_timeline_data_recomp.json", testFilePath + "/metric_timeline_recomp4.json", testFilePath + "/exp_metric_timeline4_metric_A.json", testFilePath + "/exp_metric_recomputed4.json", true);

//        DataSet<StatusTimeline> statusMetricTimeline5 = testMetricTimelines(env, params, testFilePath + "/initial_group_timeline_data_recomp5.json", testFilePath + "/metric_timeline_recomp5.json", testFilePath + "/exp_metric_timeline5_metric_A.json", testFilePath + "/exp_metric_recomputed5.json", true);

        /**************/
        DataSet<StatusTimeline> statusEndpointTimeline1 = testEndpointTimelines(env, params, testFilePath + "/initial_group_timeline_data_recomp.json", testFilePath + "/endpoint_timeline_recomp1.json", testFilePath + "/exp_endpoint_timeline1.json", true);
        DataSet<StatusTimeline> statusEndpointTimeline2 = testEndpointTimelines(env, params, testFilePath + "/initial_group_timeline_data_recomp2.json", testFilePath + "/endpoint_timeline_recomp2.json", testFilePath + "/exp_endpoint_timeline2.json", true);

        /**************/
        DataSet<StatusTimeline> statusServiceTimeline1 = testServiceTimelines(env, params, testFilePath + "/initial_group_timeline_data_recomp.json", testFilePath + "/service_timeline_recomp1.json", testFilePath + "/exp_service_timeline1.json", true);


        /**************/

        testGroupTimelines(env, params, testFilePath + "/initial_group_timeline_data_recomp.json", testFilePath + "/group_timeline_recomp1.json", testFilePath + "/exp_group_timeline1.json", true);
        testGroupTimelines(env, params, testFilePath + "/initial_group_timeline_data_recomp2.json", testFilePath + "/group_timeline_recomp2.json", testFilePath + "/exp_group_timeline2.json", true);


        env.execute();


    }

    private static void buildProfileData(ExecutionEnvironment env, String recompFile) throws Exception {

        amr = mockAmr(recompFile);

        cfgDS = env.fromElements(amr.getResourceJSON(ApiResource.CONFIG));

        confDS = env.fromElements(amr.getResourceJSON(ApiResource.CONFIG));
        // Get conf data
        List<String> confData = confDS.collect();
        ReportManager cfgMgr = new ReportManager();
        cfgMgr.loadJsonString(confData);

        //  enableComputations(cfgMgr.activeComputations, params);
        apsDS = env.fromElements(amr.getResourceJSON(ApiResource.AGGREGATION));
        opsDS = env.fromElements(amr.getResourceJSON(ApiResource.OPS));

        opsManager = new OperationsManager();
        opsManager.loadJsonString(opsDS.collect());

        mtagsDS = env.fromElements("");
        if (amr.getResourceJSON(ApiResource.MTAGS) != null) {

            mtagsDS = env.fromElements(amr.getResourceJSON(ApiResource.MTAGS));
        }

        weightDS = env.fromElements(new Weight());
        Weight[] listWeights = new Weight[0];

        downDS = env.fromElements(new Downtime());
        // begin with empty threshold datasource

        thrDS = env.fromElements("");
        if (amr.getResourceJSON(ApiResource.THRESHOLDS) != null) {
            thrDS = env.fromElements(amr.getResourceJSON(ApiResource.THRESHOLDS));
        }

        ReportManager confMgr = new ReportManager();
        confMgr.loadJsonString(cfgDS.collect());

        // Get conf data
        nempsDS = env.fromElements(new MetricProfile("", "", "", null));

        mpsDS = env.fromElements(amr.getListMetrics());
        egpDS = env.fromElements(amr.getListGroupEndpoints());
        ggpDS = env.fromElements(new GroupGroup());
        GroupGroup[] listGroups = amr.getListGroupGroups();
        if (listGroups.length > 0) {
            ggpDS = env.fromElements(amr.getListGroupGroups());
        }

        Downtime[] listDowntimes = amr.getListDowntimes();
        if (listDowntimes.length > 0) {
            downDS = env.fromElements(amr.getListDowntimes());
        }
        opsJson = new ArrayList();
        opsJson.add(amr.getResourceJSON(ApiResource.OPS));
        aggrJson = new ArrayList();
        aggrJson.add(amr.getResourceJSON(ApiResource.AGGREGATION));

        recDS = env.fromElements("");
        if (amr.getResourceJSON(ApiResource.RECOMPUTATIONS) != null) {
            recDS = env.fromElements(amr.getResourceJSON(ApiResource.RECOMPUTATIONS));
        }

    }


    private static DataSet<StatusTimeline> testMetricTimelines(ExecutionEnvironment env, ParameterTool params, String initialDataFile, String recomputationFile, String expResFile, String expMetricRecomputedFile, boolean compareEquals) throws Exception {

        buildProfileData(env, recomputationFile);

        URL calcPrevStatusURL = ArgoMultiJob.class.getResource(initialDataFile);
        DataSet<String> calcPrevStatusString = env.readTextFile(calcPrevStatusURL.toString());
        List<String> calcPrevStatusList = calcPrevStatusString.collect();
        DataSet<StatusMetric> prevData = env.fromElements(getListStatusMetric(calcPrevStatusList.get(0)));

        DataSet<StatusMetric> recomputedData = prevData.groupBy("group", "service", "hostname", "metric").sortGroup("timestamp", Order.ASCENDING)
                .reduceGroup(new CalcRecomputation(params)).withBroadcastSet(mpsDS, "mps").withBroadcastSet(opsDS, "ops")
                .withBroadcastSet(apsDS, "aps").withBroadcastSet(recDS, "rec");
        if (compareEquals) {
            URL calcRecomputedURL = ArgoMultiJob.class.getResource(expMetricRecomputedFile);
            DataSet<String> calcRecomputedString = env.readTextFile(calcRecomputedURL.toString());
            List<String> calcRecomputedList = calcRecomputedString.collect();
            DataSet<StatusMetric> expRecomputedData = env.fromElements(getListStatusMetric(calcRecomputedList.get(0)));

            Assert.assertEquals(TestUtils.compareLists(expRecomputedData.collect(), recomputedData.collect()), true);
        }
        DataSet<StatusTimeline> statusMetricTimeline = recomputedData.distinct("group", "service", "hostname", "metric", "status", "timestamp").groupBy("group", "service", "hostname", "metric").sortGroup("timestamp", Order.ASCENDING)
                .reduceGroup(new CalcMetricTimeline(params)).withBroadcastSet(mpsDS, "mps").withBroadcastSet(opsDS, "ops")
                .withBroadcastSet(apsDS, "aps").withBroadcastSet(recDS, "rec");

        if (compareEquals) {

            URL expMetricTimelineURL = ArgoMultiJob.class.getResource(expResFile);
            DataSet<String> expMetricTimelineString = env.readTextFile(expMetricTimelineURL.toString());
            List<String> expMetricTimelineList = expMetricTimelineString.collect();
            OperationsManager opsMgr = new OperationsManager();
            opsMgr.loadJsonString(opsJson);
            List<StatusTimeline> expMetricTimelines = new ArrayList();
            if (!expMetricTimelineList.isEmpty()) {
                DataSet<StatusTimeline> expMetricTimelineDS = env.fromElements(getListExpTimelines(expMetricTimelineList.get(0), opsMgr));
                expMetricTimelines = expMetricTimelineDS.collect();
            }

            Assert.assertEquals(TestUtils.compareLists(expMetricTimelines, statusMetricTimeline.collect()), true);
        }

        prevData.output(new DiscardingOutputFormat<StatusMetric>());
        return statusMetricTimeline;
    }

    private static TreeMap<String, List<StatusMetric>> prepareLevelStatusMetrics(List<StatusMetric> statusMetricList, RecomputationsManager.ElementType elementType) {
        TreeMap<String, List<StatusMetric>> map = new TreeMap<>();
        String key = "";
        for (StatusMetric st : statusMetricList) {
            switch (elementType) {
                case GROUP:
                    key = st.getGroup();
                    break;
                case SERVICE:
                    key = st.getService();
                    break;
                case ENDPOINT:
                    key = st.getHostname();
                    break;
                case METRIC:
                    key = st.getMetric();
                    break;
                default:
                    key = "";
                    break;
            }
            List<StatusMetric> list = new ArrayList<>();
            if (map.containsKey(key)) {
                list = map.get(key);
            }
            list.add(st);
            map.put(key, list);
        }

        return map;
    }

    private static List<StatusMetric> buildExpectedTimeline(ExecutionEnvironment env, String expDataFile, boolean hasThr) throws Exception {
        URL expStatusURL = ArgoMultiJob.class.getResource(expDataFile);
        DataSet<String> expStatusString = env.readTextFile(expStatusURL.toString());
        List<String> expStatusList = expStatusString.collect();
        List<StatusMetric> expStatusRes = new ArrayList();
        if (!expStatusList.isEmpty()) {
            DataSet<StatusMetric> expStatusDS = env.fromElements(getListCalcPrevData(expStatusList.get(0), hasThr));
            expStatusRes = prepareStatusData(expStatusDS.collect());
        }

        return expStatusRes;
    }

    private static DataSet<StatusTimeline> testEndpointTimelines(ExecutionEnvironment env, ParameterTool params, String initialDataFile, String recomputationFile, String expResFile, boolean compareEquals) throws Exception {


        DataSet<StatusTimeline> statusMetricTimeline = testMetricTimelines(env, params, initialDataFile, recomputationFile, null, null, false);
        DataSet<StatusTimeline> statusEndpointTimeline = statusMetricTimeline.groupBy("group", "service", "hostname")
                .reduceGroup(new CalcEndpointTimeline(params, now)).withBroadcastSet(mpsDS, "mps").withBroadcastSet(opsDS, "ops")
                .withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
                .withBroadcastSet(apsDS, "aps").withBroadcastSet(downDS, "down").withBroadcastSet(recDS, "rec");

        if (compareEquals) {
            URL expEndpointTimelineURL = ArgoMultiJob.class.getResource(expResFile);
            DataSet<String> expEndpointString = env.readTextFile(expEndpointTimelineURL.toString());
            List<String> expEndpointList = expEndpointString.collect();
            OperationsManager opsMgr = new OperationsManager();
            opsMgr.loadJsonString(opsJson);
            List<StatusTimeline> expEndpointTimelines = new ArrayList();
            if (!expEndpointList.isEmpty()) {
                DataSet<StatusTimeline> expEndpointTimelineDS = env.fromElements(getListExpTimelines(expEndpointList.get(0), opsMgr));
                expEndpointTimelines = expEndpointTimelineDS.collect();
            }
            Assert.assertEquals(TestUtils.compareLists(expEndpointTimelines, statusEndpointTimeline.collect()), true);
        }
        statusEndpointTimeline.output(new DiscardingOutputFormat<StatusTimeline>());
        return statusEndpointTimeline;
    }


    private static DataSet<StatusTimeline> testServiceTimelines(ExecutionEnvironment env, ParameterTool params, String initialDataFile, String recomputationFile, String expResFile, boolean compareEquals) throws Exception {

        DataSet<StatusTimeline> statusEndpointTimeline = testEndpointTimelines(env, params, initialDataFile, recomputationFile, null, false);
        DataSet<StatusTimeline> statusServiceTimeline = statusEndpointTimeline.groupBy("group", "service")
                .reduceGroup(new CalcServiceTimeline(params)).withBroadcastSet(mpsDS, "mps").withBroadcastSet(opsDS, "ops")
                .withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
                .withBroadcastSet(apsDS, "aps").withBroadcastSet(recDS, "rec");

        if (compareEquals) {

            URL expServiceTimelineURL = ArgoMultiJob.class.getResource(expResFile);
            DataSet<String> expServiceString = env.readTextFile(expServiceTimelineURL.toString());
            List<String> expServiceList = expServiceString.collect();
            OperationsManager opsMgr = new OperationsManager();
            opsMgr.loadJsonString(opsJson);
            List<StatusTimeline> expServiceTimelines = new ArrayList();
            if (!expServiceList.isEmpty()) {
                DataSet<StatusTimeline> expServiceTimelineDS = env.fromElements(getListExpTimelines(expServiceList.get(0), opsMgr));
                expServiceTimelines = expServiceTimelineDS.collect();
            }
            Assert.assertEquals(TestUtils.compareLists(expServiceTimelines, statusServiceTimeline.collect()), true);
        }
        statusServiceTimeline.output(new DiscardingOutputFormat<StatusTimeline>());
        return statusServiceTimeline;
    }

    private static DataSet<StatusTimeline> testGroupTimelines(ExecutionEnvironment env, ParameterTool params, String initialDataFile, String recomputationFile, String expResFile, boolean compareEquals) throws Exception {

        DataSet<StatusTimeline> statusServiceTimeline = testServiceTimelines(env, params, initialDataFile, recomputationFile, null, false);

        DataSet<StatusTimeline> statusEndGroupFunctionTimeline = statusServiceTimeline.groupBy("group", "function")
                .reduceGroup(new CalcGroupFunctionTimeline(params)).withBroadcastSet(mpsDS, "mps").withBroadcastSet(opsDS, "ops")
                .withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
                .withBroadcastSet(apsDS, "aps");


        DataSet<StatusTimeline> statusGroupTimeline = statusEndGroupFunctionTimeline.groupBy("group")
                .reduceGroup(new CalcGroupTimeline(params)).withBroadcastSet(mpsDS, "mps").withBroadcastSet(opsDS, "ops")
                .withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
                .withBroadcastSet(apsDS, "aps").withBroadcastSet(recDS, "rec");


        if (compareEquals) {
            URL expGroupTimelineURL = ArgoMultiJob.class.getResource(expResFile);
            DataSet<String> expGroupString = env.readTextFile(expGroupTimelineURL.toString());
            List<String> expGroupList = expGroupString.collect();
            OperationsManager opsMgr = new OperationsManager();
            opsMgr.loadJsonString(opsJson);
            List<StatusTimeline> expGroupTimelines = new ArrayList();
            if (!expGroupList.isEmpty()) {
                DataSet<StatusTimeline> expGroupTimelineDS = env.fromElements(getListExpTimelines(expGroupList.get(0), opsMgr));
                expGroupTimelines = expGroupTimelineDS.collect();
            }
            Assert.assertEquals(TestUtils.compareLists(expGroupTimelines, statusGroupTimeline.collect()), true);
        }
        statusGroupTimeline.output(new DiscardingOutputFormat<StatusTimeline>());
        return statusGroupTimeline;
    }

    public static String loadResJSON(String resURL) {

        InputStream jsonInputStream = ArgoMultiJobTest.class
                .getResourceAsStream(resURL);
        String content = new BufferedReader(new InputStreamReader(jsonInputStream, StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n"));
        return content;

    }


    private static ApiResourceManager mockAmr(String recompFile) {

        amr = new ApiResourceManager("localhost:8443", "s3cr3t");

        String aggrData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/recomputations/aggregations.json"), false);
        String opsData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/operations.json"), false);
        String configData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/config.json"), false);
        String mprofileData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/metric_profiles.json"), false);
        String egpData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/topology_endpoints.json"), true);
        String ggpData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/topology_groups.json"), true);
        String downtimeData = amr.getApiResponseParser().getJsonData(loadResJSON("/test/downtimes.json"), false);
        String recompData = amr.getApiResponseParser().getJsonData(loadResJSON(recompFile), true);
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

    public boolean isOFF(ParameterTool params, String paramName) {
        if (params.has(paramName)) {
            if (params.get(paramName).equals("OFF")) {
                return false;

            }
        }
        return true;

    }

    private static StatusMetric[] getListStatusMetric(String content) {
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
            String message = "";
            if (jItem.has("message") && !jItem.get("message").isJsonNull()) {
                message = jItem.get("message").getAsString();
            }
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
            stm.setPrevTs(jItem.get("prevTs").getAsString());
            stm.setPrevState(jItem.get("prevState").getAsString());
            stm.setMessage(message);

            String timestamp2 = stm.getTimestamp().split("Z")[0];
            String[] tsToken = timestamp2.split("T");
            int dateInt = Integer.parseInt(tsToken[0].replace("-", ""));
            int timeInt = Integer.parseInt(tsToken[1].replace(":", ""));
            stm.setDateInt(dateInt);
            stm.setTimeInt(timeInt);

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
    private static StatusMetric[] getListCalcPrevData(String content, boolean hasThr) {

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

    private static List<StatusMetric> prepareStatusData(List<StatusMetric> input) {

        for (StatusMetric sm : input) {
            sm.setInfo("");
            sm.setMessage("");
            sm.setSummary("");
            sm.setTags("");
            sm.setActualData("");
            sm.setRuleApplied("");
            sm.setOgStatus("");
            String timestamp2 = sm.getTimestamp().split("Z")[0];
            String[] tsToken = timestamp2.split("T");
            int dateInt = Integer.parseInt(tsToken[0].replace("-", ""));
            sm.setDateInt(dateInt);
            //   sm.setTimeInt(timeInt);
        }
        return input;
    }

    private static List<StatusTimeline> prepareStatusTimeline(TreeMap<String, List<StatusMetric>> map, OperationsManager opsManager) throws ParseException {
        List<StatusTimeline> statusTimelines = new ArrayList<>();
        for (String key : map.keySet()) {
            //StatusMetric element = data.get(0);
            StatusTimeline timeline = new StatusTimeline();

            timeline.setGroup(map.get(key).get(0).getGroup());
            timeline.setFunction(map.get(key).get(0).getFunction());
            timeline.setService(map.get(key).get(0).getService());
            timeline.setHostname(map.get(key).get(0).getHostname());
            timeline.setMetric(map.get(key).get(0).getMetric());
            timeline.setHasThr(map.get(key).get(0).getHasThr());
            ArrayList<TimeStatus> timestatusList = new ArrayList<>();

            for (StatusMetric dataElement : map.get(key)) {
                TimeStatus timeStatus = new TimeStatus();
                timeStatus.setStatus(opsManager.getIntStatus(dataElement.getStatus()));
                DateTime dt = Utils.convertStringtoDate("yyyy-MM-dd'T'HH:mm:ss'Z'", dataElement.getTimestamp());
                timeStatus.setTimestamp(dt.getMillis());
                timestatusList.add(timeStatus);
            }
            timeline.setTimestamps(timestatusList);

            statusTimelines.add(timeline);
        }
        return statusTimelines;
    }


    public static StatusTimeline[] getListExpTimelines(String content, OperationsManager opsMgr) throws ParseException {
        List<StatusTimeline> results = new ArrayList<StatusTimeline>();
        JsonParser jsonParser = new JsonParser();
        JsonElement jElement = jsonParser.parse(content);
        JsonArray jRoot = jElement.getAsJsonArray();
        for (int i = 0; i < jRoot.size(); i++) {
            JsonObject jItem = jRoot.get(i).getAsJsonObject();

            String group = jItem.get("group").getAsString();
            String metric = "";
            String hostname = "";
            String service = "";
            String function = "";
            String recompRequestId = "";
            boolean hasThr = false;
            ArrayList<TimeStatus> timeStatusList = new ArrayList<>();

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

            if (jItem.has("recompRequestId")) {
                recompRequestId = jItem.get("recompRequestId").getAsString();

            }
            if (jItem.has("hasThr")) {
                hasThr = jItem.get("hasThr").getAsBoolean();

            }

            JsonArray statusHistory = jItem.get("status_history").getAsJsonArray();

            for (int l = 0; l < statusHistory.size(); l++) {
                JsonObject jStatusHistory = statusHistory.get(l).getAsJsonObject();
                String status = jStatusHistory.get("status").getAsString();
                String timestamp = jStatusHistory.get("timestamp").getAsString();

                long ts = Utils.convertStringtoDate("yyyy-MM-dd'T'HH:mm:ss'Z'", timestamp).getMillis();
                int statusInt = opsMgr.getIntStatus(status);

                timeStatusList.add(new TimeStatus(ts, statusInt));

            }
            StatusTimeline statusTimeline = new StatusTimeline(group, function, service, hostname, metric, timeStatusList);
            statusTimeline.setHasThr(hasThr);
            results.add(statusTimeline);
        }

        StatusTimeline[] rArr = new StatusTimeline[results.size()];
        rArr = results.toArray(rArr);
        return rArr;
    }


}
