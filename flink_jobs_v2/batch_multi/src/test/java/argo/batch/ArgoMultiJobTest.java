package argo.batch;

import argo.amr.ApiResource;
import argo.amr.ApiResourceManager;
import argo.ar.*;
import argo.avro.*;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;
import profilesmanager.OperationsManager;
import profilesmanager.RecomputationsManager;
import profilesmanager.ReportManager;
import trends.calculations.*;
import trends.flipflops.*;
import trends.status.EndpointTrendsCounter;
import trends.status.GroupTrendsCounter;
import trends.status.MetricTrendsCounter;
import trends.status.ServiceTrendsCounter;
import utils.Utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * * ArgoMultiJobTest tests the ArgoStatusBatch implementation comparing for
 * each step of calculations the generated datasets and compares them with input
 * expected datasets
 */
public class ArgoMultiJobTest {

    private static boolean calcStatus = false;
    private static boolean calcAR = false;
    private static boolean calcStatusTrends = false;
    private static boolean calcFlipFlops = false;
    private static boolean calcTagTrends = false;
    private static ApiResourceManager amr;


    private static DataSource<String> cfgDS;

    private static DataSource<String> confDS;
    // Get conf data

    private static DataSource<String> apsDS;
    private static DataSource<String> opsDS;
    private static DataSource<String> recDS;
    private static DataSource<String> mtagsDS;

    private static DataSet<Weight> weightDS;

    private static DataSet<Downtime> downDS;

    private static DataSource<String> thrDS;
    // Get conf data
    private static DataSet<MetricProfile> nempsDS;

    private static DataSet<MetricProfile> mpsDS;
    private static DataSet<GroupEndpoint> egpDS;
    private static DataSet<GroupGroup> ggpDS;
    private static DateTime now = new DateTime(DateTimeZone.UTC);
    private static DataSet<StatusMetric> stDetailDS;
    private static DataSet<StatusMetric> recomputedData;
    private static DataSet<StatusTimeline> statusEndpointTimeline;
    private static DataSet<StatusTimeline> statusMetricTimeline;
    private static DataSet<StatusTimeline> statusServiceTimeline;
    private static DataSet<StatusTimeline> statusGroupTimeline;
    private static DataSet<StatusTimeline> statusEndGroupFunctionTimeline;
    private static ReportManager confMgr;
    private static ReportManager cfgMgr;

    private static DataSource<HashMap<String, List<RecomputationsManager.RecomputationElement>>> metricRecomputedDS;
    private static DataSource<HashMap<String, List<RecomputationsManager.RecomputationElement>>> endpointRecomputedDS;
    private static DataSource<HashMap<String, List<RecomputationsManager.RecomputationElement>>> serviceRecomputedDS;
    private static DataSource<HashMap<String, List<RecomputationsManager.RecomputationElement>>> groupRecomputedDS;
    private static DataSource<Map<String, ArrayList<Map<String, Date>>>> monEngineRecDS;
    private static DataSource<Map<String, ArrayList<Map<String, String>>>> groupsRecDS;

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

        amr = mockAmr();

        cfgDS = env.fromElements(amr.getResourceJSON(ApiResource.CONFIG));

        confDS = env.fromElements(amr.getResourceJSON(ApiResource.CONFIG));
        // Get conf data 
        List<String> confData = confDS.collect();
        cfgMgr = new ReportManager();
        cfgMgr.loadJsonString(confData);

        enableComputations(cfgMgr.activeComputations, params);
        apsDS = env.fromElements(amr.getResourceJSON(ApiResource.AGGREGATION));
        opsDS = env.fromElements(amr.getResourceJSON(ApiResource.OPS));


        recDS = env.fromElements("");
        if (amr.getResourceJSON(ApiResource.RECOMPUTATIONS) != null) {
            recDS = env.fromElements(amr.getResourceJSON(ApiResource.RECOMPUTATIONS));
        }
        RecomputationsManager.loadJsonString(recDS.collect());

        metricRecomputedDS = env.fromElements(RecomputationsManager.metricRecomputationItems);
        endpointRecomputedDS = env.fromElements(RecomputationsManager.endpointRecomputationItems);
        serviceRecomputedDS = env.fromElements(RecomputationsManager.serviceRecomputationItems);
        groupRecomputedDS = env.fromElements(RecomputationsManager.groupRecomputationItems);
        monEngineRecDS = env.fromElements(RecomputationsManager.monEngines);
        groupsRecDS = env.fromElements(RecomputationsManager.groups);


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

        confMgr = new ReportManager();
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
        URL mdataURL = ArgoMultiJobTest.class.getResource("/test/metricdata_downtime.avro");

        Path in = new Path(mdataURL.getPath());
        AvroInputFormat<MetricData> mdataAvro = new AvroInputFormat<MetricData>(in, MetricData.class);
        DataSet<MetricData> mdataDS = env.createInput(mdataAvro);

        URL pdataURL = ArgoMultiJobTest.class.getResource("/test/pdata.avro");

        Path pin = new Path(pdataURL.getPath());
        AvroInputFormat<MetricData> pdataAvro = new AvroInputFormat<MetricData>(pin, MetricData.class);
        DataSet<MetricData> pdataDS = env.createInput(pdataAvro);

        DataSet<MetricData> pdataCleanDS = pdataDS.flatMap(new ExcludeMetricData()).withBroadcastSet(monEngineRecDS, "rec");
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
        Assert.assertEquals(TestUtils.compareLists(lpdataDS.collect(), pdataMin.collect()), true);

        //************* Test unioned metric data of previous and current date ************
        DataSet<MetricData> mdataPrevTotalDS = mdataDS.union(pdataMin);
        URL unionpdataURL = ArgoMultiJobTest.class.getResource("/test/uniondata_downtime.avro");
        Path unionpin = new Path(unionpdataURL.getPath());
        AvroInputFormat<MetricData> unionpdataAvro = new AvroInputFormat(unionpin, MetricData.class);
        DataSet<MetricData> unionpdataDS = env.createInput(unionpdataAvro);

        Assert.assertEquals(TestUtils.compareLists(unionpdataDS.collect(), mdataPrevTotalDS.collect()), true);

        //***************************Test FillMIssing
        DataSet<StatusMetric> fillMissDS = mdataPrevTotalDS.reduceGroup(new FillMissing(params))
                .withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
                .withBroadcastSet(opsDS, "ops").withBroadcastSet(cfgDS, "conf").withBroadcastSet(nempsDS, "nemps")
                .withBroadcastSet(apsDS, "aps");

        URL expFillMissdataURL = ArgoMultiJobTest.class.getResource("/test/fillmissing.json");
        DataSet<String> fillMissString = env.readTextFile(expFillMissdataURL.toString());
        List<String> fillMissingList = fillMissString.collect();
        List<StatusMetric> expFillMissRes = new ArrayList();
        if (!fillMissingList.isEmpty()) {
            DataSet<StatusMetric> expFillMissDS = env.fromElements(getListStatusMetric(fillMissingList.get(0)));
            expFillMissRes = prepareInputData(expFillMissDS.collect());
        }
        Assert.assertEquals(TestUtils.compareLists(expFillMissRes, fillMissDS.collect()), true);
        //**************** Test PickEndpoints
        DataSet<StatusMetric> mdataTrimDS = mdataPrevTotalDS.flatMap(new PickEndpoints(params))
                .withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
                .withBroadcastSet(monEngineRecDS, "rec").withBroadcastSet(cfgDS, "conf").withBroadcastSet(thrDS, "thr")
                .withBroadcastSet(opsDS, "ops").withBroadcastSet(apsDS, "aps");

        URL expPickdataURL = ArgoMultiJobTest.class.getResource("/test/pickdata.json");
        DataSet<String> expPickDataString = env.readTextFile(expPickdataURL.toString());
        List<String> pickDataList = expPickDataString.collect();
        List<StatusMetric> expPickDataRes = new ArrayList();
        if (!pickDataList.isEmpty()) {
            DataSet<StatusMetric> expPickDataDS = env.fromElements(getListStatusMetric(pickDataList.get(0)));
            expPickDataRes = preparePickData(expPickDataDS.collect());
        }

        Assert.assertEquals(TestUtils.compareLists(expPickDataRes, mdataTrimDS.collect()), true);

        DataSet<StatusMetric> mdataTotalDS = mdataTrimDS.union(fillMissDS);
        Assert.assertEquals(TestUtils.compareLists(expPickDataRes, mdataTotalDS.collect()), true);

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
        Assert.assertEquals(TestUtils.compareLists(expMapServicesRes, mdataTotalDS.collect()), true);
        //************** Test CalcPrevStatus
        stDetailDS = mdataTotalDS.groupBy("group", "service", "hostname", "metric")
                .sortGroup("timestamp", Order.ASCENDING).reduceGroup(new CalcPrevStatus(params))
                .withBroadcastSet(mpsDS, "mps").withBroadcastSet(opsDS, "ops");

        URL calcPrevStatusURL = ArgoMultiJob.class.getResource("/test/expCalcPrevStatus.json");
        DataSet<String> calcPrevStatusString = env.readTextFile(calcPrevStatusURL.toString());
        List<String> calcPrevStatusList = calcPrevStatusString.collect();
        List<StatusMetric> expCalcPrevStatusRes = new ArrayList();
        if (!calcPrevStatusList.isEmpty()) {
            DataSet<StatusMetric> expCalcPrepStatusDS = env.fromElements(getListCalcPrevData(calcPrevStatusList.get(0)));
            expCalcPrevStatusRes = preparePickData(expCalcPrepStatusDS.collect());
        }
        Assert.assertEquals(TestUtils.compareLists(expCalcPrevStatusRes, stDetailDS.collect()), true);

        testTimelines(env, expCalcPrevStatusRes, params);
        testTrendsAndFlipFlops(env, params);
        stDetailDS.output(new DiscardingOutputFormat<StatusMetric>());
        env.execute();

    }

    public static void testTimelines(ExecutionEnvironment env, List<StatusMetric> expCalcPrevStatusRes, ParameterTool params) throws Exception {
        //*************** Test Metric Timeline
        //Create StatusMetricTimeline dataset for endpoints
        recomputedData = stDetailDS.groupBy("group", "service", "hostname", "metric").sortGroup("timestamp", Order.ASCENDING)
                .reduceGroup(new CalcRecomputation(params)).withBroadcastSet(mpsDS, "mps").withBroadcastSet(opsDS, "ops")
                .withBroadcastSet(apsDS, "aps").withBroadcastSet(metricRecomputedDS, "rec");

        URL calcRecompDataURL = ArgoMultiJob.class.getResource("/test/exp_recomp_data.json");
        DataSet<String> calcRecompDataString = env.readTextFile(calcRecompDataURL.toString());
        List<String> calcRecompDataList = calcRecompDataString.collect();
        List<StatusMetric> expCalcRecompDataList = new ArrayList();
        if (!calcRecompDataList.isEmpty()) {
            DataSet<StatusMetric> expCalcRecompDataDS = env.fromElements(getListCalcPrevData(calcRecompDataList.get(0)));
            expCalcRecompDataList = preparePickData(expCalcRecompDataDS.collect());
        }

        Assert.assertEquals(TestUtils.compareLists(expCalcRecompDataList, recomputedData.collect()), true);

        statusMetricTimeline = recomputedData.groupBy("group", "service", "hostname", "metric").sortGroup("timestamp", Order.ASCENDING)
                .reduceGroup(new CalcMetricTimeline(params)).withBroadcastSet(mpsDS, "mps").withBroadcastSet(opsDS, "ops")
                .withBroadcastSet(apsDS, "aps");

        ArrayList<String> opsJson = new ArrayList();
        opsJson.add(amr.getResourceJSON(ApiResource.OPS));

        URL expMetricTimelineURL = ArgoMultiJob.class.getResource("/test/expMetricTimeline.json");
        DataSet<String> expMetricTimelineString = env.readTextFile(expMetricTimelineURL.toString());
        List<String> expMetricTimelineList = expMetricTimelineString.collect();
        OperationsManager opsMgr = new OperationsManager();
        opsMgr.loadJsonString(opsJson);
        List<StatusTimeline> expMetricTimelines = new ArrayList();
        if (!calcRecompDataList.isEmpty()) {
            DataSet<StatusTimeline> expMetricTimelineDS = env.fromElements(getListExpTimelines(expMetricTimelineList.get(0), opsMgr));
            expMetricTimelines = expMetricTimelineDS.collect();
        }

        Assert.assertEquals(TestUtils.compareLists(expMetricTimelines, statusMetricTimeline.collect()), true);

        //*************** Test Endpoint  Timeline
        //Create StatusMetricTimeline dataset for endpoints
        statusEndpointTimeline = statusMetricTimeline.groupBy("group", "service", "hostname")
                .reduceGroup(new CalcEndpointTimeline(params, now)).withBroadcastSet(mpsDS, "mps").withBroadcastSet(opsDS, "ops")
                .withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
                .withBroadcastSet(apsDS, "aps").withBroadcastSet(downDS, "down").withBroadcastSet(endpointRecomputedDS, "rec");

        ArrayList<String> aggrJson = new ArrayList();
        aggrJson.add(amr.getResourceJSON(ApiResource.AGGREGATION));

        URL expEndpointTimelineURL = ArgoMultiJob.class.getResource("/test/expEndpointTimeline.json");
        DataSet<String> expEndpointTimelineString = env.readTextFile(expEndpointTimelineURL.toString());
        List<String> expEndpointTimelineList = expEndpointTimelineString.collect();
        List<StatusTimeline> expEndpointTimelines = new ArrayList();
        if (!expEndpointTimelineList.isEmpty()) {
            DataSet<StatusTimeline> expEndpointTimelineDS = env.fromElements(getListExpTimelines(expEndpointTimelineList.get(0), opsMgr));
            expEndpointTimelines = expEndpointTimelineDS.collect();
        }

        Assert.assertEquals(TestUtils.compareLists(expEndpointTimelines, statusEndpointTimeline.collect()), true);


        statusServiceTimeline = statusEndpointTimeline.groupBy("group", "service")
                .reduceGroup(new CalcServiceTimeline(params)).withBroadcastSet(mpsDS, "mps").withBroadcastSet(opsDS, "ops")
                .withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
                .withBroadcastSet(apsDS, "aps").withBroadcastSet(serviceRecomputedDS, "rec");

        URL expServiceTimelineURL = ArgoMultiJob.class.getResource("/test/expServiceTimeline.json");
        DataSet<String> expServiceTimelineString = env.readTextFile(expServiceTimelineURL.toString());
        List<String> expServiceTimelineList = expServiceTimelineString.collect();
        List<StatusTimeline> expServiceTimelines = new ArrayList();
        if (!expEndpointTimelineList.isEmpty()) {
            DataSet<StatusTimeline> expServiceTimelineDS = env.fromElements(getListExpTimelines(expServiceTimelineList.get(0), opsMgr));
            expServiceTimelines = expServiceTimelineDS.collect();
        }
        Assert.assertEquals(TestUtils.compareLists(expServiceTimelines, statusServiceTimeline.collect()), true);

        //*************** Test Function Timeline
        statusEndGroupFunctionTimeline = statusServiceTimeline.groupBy("group", "function")
                .reduceGroup(new CalcGroupFunctionTimeline(params)).withBroadcastSet(mpsDS, "mps").withBroadcastSet(opsDS, "ops")
                .withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
                .withBroadcastSet(apsDS, "aps");

        URL expFunctionTimelineURL = ArgoMultiJob.class.getResource("/test/expFunctionTimeline.json");
        DataSet<String> expFunctionString = env.readTextFile(expFunctionTimelineURL.toString());
        List<String> expFunctionlineList = expFunctionString.collect();
        List<StatusTimeline> expFunctionTimelines = new ArrayList();
        if (!expFunctionlineList.isEmpty()) {
            DataSet<StatusTimeline> expFunctionTimelineDS = env.fromElements(getListExpTimelines(expFunctionlineList.get(0), opsMgr));
            expFunctionTimelines = expFunctionTimelineDS.collect();
        }

        Assert.assertEquals(TestUtils.compareLists(expFunctionTimelines, statusEndGroupFunctionTimeline.collect()), true);

        //*************** Test Group Timeline
        statusGroupTimeline = statusEndGroupFunctionTimeline.groupBy("group")
                .reduceGroup(new CalcGroupTimeline(params)).withBroadcastSet(mpsDS, "mps").withBroadcastSet(opsDS, "ops")
                .withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
                .withBroadcastSet(apsDS, "aps").withBroadcastSet(groupRecomputedDS, "rec");

        URL expEndpointGroupTimelineURL = ArgoMultiJob.class.getResource("/test/expEndpointGroupTimeline.json");
        DataSet<String> expEndpointGroupString = env.readTextFile(expEndpointGroupTimelineURL.toString());
        List<String> expEndpointGrouplineList = expEndpointGroupString.collect();
        List<StatusTimeline> expEndpointGroupTimelines = new ArrayList();
        if (!expFunctionlineList.isEmpty()) {
            DataSet<StatusTimeline> exEndpointGroupTimelineDS = env.fromElements(getListExpTimelines(expEndpointGrouplineList.get(0), opsMgr));
            expEndpointGroupTimelines = exEndpointGroupTimelineDS.collect();
        }

        Assert.assertEquals(TestUtils.compareLists(expEndpointGroupTimelines, statusGroupTimeline.collect()), true);

    }

    private void testTrendsAndFlipFlops(ExecutionEnvironment env, ParameterTool params) throws Exception {

        if (calcStatus) {
            //Calculate endpoint timeline timestamps
            recomputedData = recomputedData.flatMap(new MapStatusMetricTags(calcAR)).withBroadcastSet(mtagsDS, "mtags").withBroadcastSet(egpDS, "egp")
                    .withBroadcastSet(cfgDS, "conf");

            URL expMTagsURL = ArgoMultiJobTest.class.getResource("/test/expmtagsdata.json");
            DataSet<String> expectedMTagsString = env.readTextFile(expMTagsURL.toString());
            List<String> expectedMTagsList = expectedMTagsString.collect();
            List<StatusMetric> expectedMTagsRes = new ArrayList();
            if (!expectedMTagsList.isEmpty()) {
                DataSet<StatusMetric> expMTagsDS = env.fromElements(getListCalcPrevData(expectedMTagsList.get(0)));
                expectedMTagsRes = prepareMTagsData(expMTagsDS.collect());
            }
            Assert.assertEquals(TestUtils.compareLists(expectedMTagsRes, recomputedData.collect()), true);

            DataSet<StatusMetric> stEndpointDS = statusEndpointTimeline.flatMap(new CalcStatusEndpoint(params)).withBroadcastSet(mpsDS, "mps").withBroadcastSet(opsDS, "ops")
                    .withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
                    .withBroadcastSet(apsDS, "aps").withBroadcastSet(cfgDS, "conf");
// Create status service data set

            URL statusendpURL = ArgoMultiJob.class.getResource("/test/statusendpoint.json");
            DataSet<String> statusendpString = env.readTextFile(statusendpURL.toString());
            List<String> statusendpList = statusendpString.collect();
            List<StatusMetric> expStatusEndpRes = new ArrayList();
            if (!statusendpList.isEmpty()) {
                DataSet<StatusMetric> expStatusEndpDS = env.fromElements(getListCalcPrevData(statusendpList.get(0)));
                expStatusEndpRes = prepareStatusEndpointData(expStatusEndpDS.collect());

            }
            Assert.assertEquals(TestUtils.compareLists(expStatusEndpRes, stEndpointDS.collect()), true);

            DataSet<StatusMetric> stServiceDS = statusServiceTimeline.flatMap(new CalcStatusService(params)).withBroadcastSet(mpsDS, "mps")
                    .withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp").withBroadcastSet(opsDS, "ops")
                    .withBroadcastSet(apsDS, "aps");
            URL statusserviceURL = ArgoMultiJob.class.getResource("/test/statusservice.json");
            DataSet<String> statusserviceString = env.readTextFile(statusserviceURL.toString());
            List<String> statusServList = statusserviceString.collect();
            List<StatusMetric> expStatusServRes = new ArrayList();
            if (!statusServList.isEmpty()) {
                DataSet<StatusMetric> expStatusServDS = env.fromElements(getListCalcPrevData(statusServList.get(0)));
                expStatusServRes = prepareStatusData(expStatusServDS.collect());

            }

            Assert.assertEquals(TestUtils.compareLists(expStatusServRes, stServiceDS.collect()), true);
            DataSet<StatusMetric> stEndGroupDS = statusGroupTimeline.flatMap(new CalcStatusEndGroup(params))
                    .withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
                    .withBroadcastSet(opsDS, "ops").withBroadcastSet(apsDS, "aps");

            URL statusgroupURL = ArgoMultiJob.class.getResource("/test/statusgroup.json");
            DataSet<String> statusgroupString = env.readTextFile(statusgroupURL.toString());
            List<String> statusGroupList = statusgroupString.collect();

            List<StatusMetric> expStatusGroupRes = new ArrayList();
            if (!statusGroupList.isEmpty()) {
                DataSet<StatusMetric> expStatusGroupDS = env.fromElements(getListCalcPrevData(statusGroupList.get(0)));
                expStatusGroupRes = prepareStatusData(expStatusGroupDS.collect());
            }

            Assert.assertEquals(TestUtils.compareLists(expStatusGroupRes, stEndGroupDS.collect()), true);
        }

        Integer rankNum = null;
        List<String> metricExpectedData = loadExpectedDataFromFile("/test/metricstatistics.json", env);
        List<String> endpointExpectedData = loadExpectedDataFromFile("/test/endpointstatistics.json", env);
        List<String> serviceExpectedData = loadExpectedDataFromFile("/test/servicestatistics.json", env);
        List<String> groupExpectedData = loadExpectedDataFromFile("/test/groupstatistics.json", env);

        //*************** Test flip flops
        if (calcFlipFlops || calcStatusTrends) {
            DataSet<MetricTrends> metricTrends = statusMetricTimeline.flatMap(new CalcMetricFlipFlopTrends());
            DataSet<EndpointTrends> endpointTrends = statusEndpointTimeline.flatMap(new CalcEndpointFlipFlopTrends());

            DataSet<ServiceTrends> serviceTrends = statusServiceTimeline.flatMap(new CalcServiceFlipFlopTrends());

            DataSet<GroupTrends> groupTrends = statusGroupTimeline.flatMap(new CalcGroupFlipFlopTrends());
            if (calcFlipFlops) {

                DataSet<MetricTrends> noZeroMetricFlipFlops = metricTrends.filter(new ZeroMetricFlipFlopFilter());

                if (rankNum != null) { //sort and rank data
                    noZeroMetricFlipFlops = noZeroMetricFlipFlops.sortPartition("flipflops", Order.DESCENDING).setParallelism(1).first(rankNum);
                } else {
                    noZeroMetricFlipFlops = noZeroMetricFlipFlops.sortPartition("flipflops", Order.DESCENDING).setParallelism(1);
                }

                DataSet<Trends> trends = noZeroMetricFlipFlops.map(new MapMetricTrends(calcTagTrends)).withBroadcastSet(mtagsDS, "mtags");

                List<Trends> expMetricStatisticRes = loadExpectedFlipFlopData(metricExpectedData, TestUtils.LEVEL.METRIC, env);

                Assert.assertEquals(TestUtils.compareLists(expMetricStatisticRes, trends.collect()), true);

                DataSet<EndpointTrends> nonZeroEndpointFlipFlops = endpointTrends.filter(new ZeroEndpointFlipFlopFilter());

                if (rankNum != null) { //sort and rank data
                    nonZeroEndpointFlipFlops = nonZeroEndpointFlipFlops.sortPartition("flipflops", Order.DESCENDING).setParallelism(1).first(rankNum);
                } else {
                    nonZeroEndpointFlipFlops = nonZeroEndpointFlipFlops.sortPartition("flipflops", Order.DESCENDING).setParallelism(1);
                }

                trends = nonZeroEndpointFlipFlops.map(new MapEndpointTrends());
                List<Trends> expEndpStatisticRes = loadExpectedFlipFlopData(endpointExpectedData, TestUtils.LEVEL.HOSTNAME, env);

                Assert.assertEquals(TestUtils.compareLists(expEndpStatisticRes, trends.collect()), true);

                DataSet<ServiceTrends> noZeroServiceFlipFlops = serviceTrends.filter(new ZeroServiceFlipFlopFilter());

                if (rankNum != null) { //sort and rank data
                    noZeroServiceFlipFlops = noZeroServiceFlipFlops.sortPartition("flipflops", Order.DESCENDING).setParallelism(1).first(rankNum);
                } else {
                    noZeroServiceFlipFlops = noZeroServiceFlipFlops.sortPartition("flipflops", Order.DESCENDING).setParallelism(1);
                }

                trends = noZeroServiceFlipFlops.map(new MapServiceTrends());
                List<Trends> expServStatisticRes = loadExpectedFlipFlopData(serviceExpectedData, TestUtils.LEVEL.SERVICE, env);

                Assert.assertEquals(TestUtils.compareLists(expServStatisticRes, trends.collect()), true);

                DataSet<GroupTrends> noZeroGroupFlipFlops = groupTrends.filter(new ZeroGroupFlipFlopFilter());

                if (rankNum != null) { //sort and rank data
                    noZeroGroupFlipFlops = noZeroGroupFlipFlops.sortPartition("flipflops", Order.DESCENDING).setParallelism(1).first(rankNum);
                } else {
                    noZeroGroupFlipFlops = noZeroGroupFlipFlops.sortPartition("flipflops", Order.DESCENDING).setParallelism(1);
                }
                trends = noZeroGroupFlipFlops.map(new MapGroupTrends());
                List<Trends> expGroupStatisticRes = loadExpectedFlipFlopData(groupExpectedData, TestUtils.LEVEL.GROUP, env);

                Assert.assertEquals(TestUtils.compareLists(expGroupStatisticRes, trends.collect()), true);

            }

            if (calcStatusTrends) {
                //flatMap dataset to tuples and count the apperances of each status type to the timeline
                DataSet<Tuple8<String, String, String, String, String, Integer, Integer, String>> metricStatusTrendsData = metricTrends.flatMap(new MetricTrendsCounter()).withBroadcastSet(opsDS, "ops").withBroadcastSet(mtagsDS, "mtags");
                //filter dataset for each status type and write to mongo db
                List<Tuple8<String, String, String, String, String, Integer, Integer, String>> expMetricTrendsRes = loadExpectedTrendData(metricExpectedData, TestUtils.LEVEL.METRIC, env);

                Assert.assertEquals(TestUtils.compareLists(expMetricTrendsRes, metricStatusTrendsData.collect()), true);

                DataSet<Tuple8<String, String, String, String, String, Integer, Integer, String>> endpointStatusTrendsData = endpointTrends.flatMap(new EndpointTrendsCounter()).withBroadcastSet(opsDS, "ops");
                //filter dataset for each status type and write to mongo db
                List<Tuple8<String, String, String, String, String, Integer, Integer, String>> expEndpTrendsRes = loadExpectedTrendData(endpointExpectedData, TestUtils.LEVEL.HOSTNAME, env);
                Assert.assertEquals(TestUtils.compareLists(expEndpTrendsRes, endpointStatusTrendsData.collect()), true);
                DataSet<Tuple8<String, String, String, String, String, Integer, Integer, String>> serviceStatusTrendsData = serviceTrends.flatMap(new ServiceTrendsCounter()).withBroadcastSet(opsDS, "ops");
                //filter dataset for each status type and write to mongo db
                List<Tuple8<String, String, String, String, String, Integer, Integer, String>> expServTrendsRes = loadExpectedTrendData(serviceExpectedData, TestUtils.LEVEL.SERVICE, env);
                Assert.assertEquals(TestUtils.compareLists(expServTrendsRes, serviceStatusTrendsData.collect()), true);

                DataSet<Tuple8<String, String, String, String, String, Integer, Integer, String>> groupStatusTrendsData = groupTrends.flatMap(new GroupTrendsCounter()).withBroadcastSet(opsDS, "ops");
                //filter dataset for each status type and write to mongo db
                List<Tuple8<String, String, String, String, String, Integer, Integer, String>> expGroupTrendsRes = loadExpectedTrendData(groupExpectedData, TestUtils.LEVEL.GROUP, env);

                Assert.assertEquals(TestUtils.compareLists(expGroupTrendsRes, groupStatusTrendsData.collect()), true);
            }

        }
        if (calcAR) {
            //Calculate endpoint a/r
            DataSet<EndpointAR> endpointArDS = statusEndpointTimeline.flatMap(new CalcEndpointAR(params)).withBroadcastSet(mpsDS, "mps")
                    .withBroadcastSet(apsDS, "aps").withBroadcastSet(opsDS, "ops").withBroadcastSet(egpDS, "egp").
                    withBroadcastSet(ggpDS, "ggp").withBroadcastSet(downDS, "down").withBroadcastSet(cfgDS, "conf");
            //Calculate endpoint timeline timestamps
            List<TestUtils.ArItem> endpArData = loadExpectedArData(endpointExpectedData, TestUtils.LEVEL.HOSTNAME, env);
            List<EndpointAR> expectedEndpAr = TestUtils.prepareEndpointAR(endpArData, params.get("run.date"));

            Assert.assertEquals(TestUtils.compareLists(expectedEndpAr, endpointArDS.collect()), true);

            DataSet<ServiceAR> serviceArDS = statusServiceTimeline.flatMap(new CalcServiceAR(params)).withBroadcastSet(mpsDS, "mps")
                    .withBroadcastSet(apsDS, "aps").withBroadcastSet(opsDS, "ops").withBroadcastSet(egpDS, "egp").
                    withBroadcastSet(ggpDS, "ggp").withBroadcastSet(downDS, "down").withBroadcastSet(cfgDS, "conf");

            List<TestUtils.ArItem> servArData = loadExpectedArData(serviceExpectedData, TestUtils.LEVEL.SERVICE, env);
            List<ServiceAR> expectedServAr = TestUtils.prepareServiceAR(servArData, params.get("run.date"));

            Assert.assertEquals(TestUtils.compareLists(expectedServAr, serviceArDS.collect()), true);
            DataSet<EndpointGroupAR> endpointGroupArDS = statusGroupTimeline.flatMap(new CalcGroupAR(params, now)).withBroadcastSet(mpsDS, "mps")
                    .withBroadcastSet(apsDS, "aps").withBroadcastSet(opsDS, "ops").withBroadcastSet(egpDS, "egp").
                    withBroadcastSet(ggpDS, "ggp").withBroadcastSet(downDS, "down").withBroadcastSet(cfgDS, "conf").withBroadcastSet(weightDS, "weight").withBroadcastSet(groupsRecDS, "rec");
            List<TestUtils.ArItem> groupArData = loadExpectedArData(groupExpectedData, TestUtils.LEVEL.GROUP, env);
            List<EndpointGroupAR> expectedGroupAr = TestUtils.prepareGroupR(groupArData, params.get("run.date"), cfgMgr.ggroup, ggpDS.collect());

            Assert.assertEquals(TestUtils.compareLists(expectedGroupAr, endpointGroupArDS.collect()), true);

        }
    }

    private List<String> loadExpectedDataFromFile(String filename, ExecutionEnvironment env) throws Exception {
        URL fileURL = ArgoMultiJobTest.class.getResource(filename);
        DataSet<String> fileString = env.readTextFile(fileURL.toString());
        List<String> dataList = fileString.collect();
        return dataList;
    }

    private List<Trends> loadExpectedFlipFlopData(List<String> dataList, TestUtils.LEVEL level, ExecutionEnvironment env) throws
            Exception {
        List<Trends> dataResult = new ArrayList();

        if (!dataList.isEmpty()) {
            DataSet<TestUtils.StatisticsItem> resultDS = env.fromElements(TestUtils.getListStatisticData(dataList.get(0)));
            dataResult = TestUtils.prepareFlipFlops(resultDS.collect(), level);

        }
        return dataResult;

    }

    private List<Tuple8<String, String, String, String, String, Integer, Integer, String>> loadExpectedTrendData
            (List<String> dataList, TestUtils.LEVEL level, ExecutionEnvironment env) throws Exception {
        List<Tuple8<String, String, String, String, String, Integer, Integer, String>> dataResult = new ArrayList();

        if (!dataList.isEmpty()) {
            DataSet<TestUtils.StatisticsItem> resultDS = env.fromElements(TestUtils.getListStatisticData(dataList.get(0)));
            dataResult = TestUtils.prepareTrends(resultDS.collect(), level);

        }
        return dataResult;

    }

    private List<TestUtils.ArItem> loadExpectedArData(List<String> dataList, TestUtils.LEVEL level, ExecutionEnvironment
            env) throws Exception {
        List<TestUtils.ArItem> dataResult = new ArrayList();

        if (!dataList.isEmpty()) {
            DataSet<TestUtils.StatisticsItem> resultDS = env.fromElements(TestUtils.getListStatisticData(dataList.get(0)));
            dataResult = TestUtils.prepareAR(resultDS.collect(), level);

        }
        return dataResult;
    }

    public List<StatusMetric> prepareInputData(List<StatusMetric> input) {

        for (StatusMetric sm : input) {
            if (sm.getActualData() == null) {
                sm.setActualData("");
            }
            sm.setFunction("");
            sm.setHasThr(false);
            sm.setInfo("");
            sm.setMessage("");
            if (sm.getOgStatus() == null) {
                sm.setOgStatus("");
            }
            sm.setPrevState("");
            if (sm.getRuleApplied() == null) {
                sm.setRuleApplied("");
            }
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

    public List<StatusMetric> prepareMTagsData(List<StatusMetric> input) {

        for (StatusMetric sm : input) {
            if (sm.getActualData().equals("")) {
                sm.setActualData(null);
            }

            sm.setInfo("{}");

            if(sm.getMessage().equals("")) {
                sm.setMessage(null);
            }
            sm.setSummary(null);
            String timestamp2 = sm.getTimestamp().split("Z")[0];
            String[] tsToken = timestamp2.split("T");
            int dateInt = Integer.parseInt(tsToken[0].replace("-", ""));
            int timeInt = Integer.parseInt(tsToken[1].replace(":", ""));
            sm.setDateInt(dateInt);
            sm.setTimeInt(timeInt);
        }
        return input;
    }

    public static List<StatusMetric> preparePickData(List<StatusMetric> input) {

        for (StatusMetric sm : input) {

            if (sm.getActualData().equals("")) {
                sm.setActualData(null);
            }

            sm.setInfo("{}");
            if (sm.getMessage() == null || sm.getMessage().equals("")) {
                sm.setMessage(null);

            }
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

    public List<StatusMetric> prepareStatusEndpointData(List<StatusMetric> input) {

        for (StatusMetric sm : input) {
            sm.setInfo("{}");
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

    public List<StatusMetric> prepareStatusData(List<StatusMetric> input) {

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

    public static String loadResJSON(String resURL) {

        InputStream jsonInputStream = ArgoMultiJobTest.class
                .getResourceAsStream(resURL);
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

    /**
     * Parses the Metric profile content retrieved from argo-web-api and
     * provides a list of MetricProfile avro objects to be used in the next
     * steps of the pipeline
     */
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
            String recompRequestId = "";
            if (jItem.has("recompRequestId")) {
                recompRequestId = jItem.get("recompRequestId").getAsString();

            }
           if(group.equals("Group_X") && service.equals("Service_X")  && hostname.equals("Hostname_X") && metric.equals("Metric_X")){
               System.out.println("here");
           }

            String message = "";
            if (jItem.has("message") && !jItem.get("message").isJsonNull()) {
                message = jItem.get("message").getAsString();
            }
            if (jItem.has("hasThr") && !jItem.get("hasThr").isJsonNull()) {
                hasThr = jItem.get("hasThr").getAsBoolean();
            }
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
            stm.setMessage(message);
            results.add(stm);
        }
        StatusMetric[] rArr = new StatusMetric[results.size()];
        rArr = results.toArray(rArr);
        return rArr;
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


    private static void enableComputations(ReportManager.ActiveComputations activeComputations, ParameterTool
            params) {

        calcStatus = isOFF(params, "calcStatus", activeComputations);
        calcAR = isOFF(params, "calcAR", activeComputations);
        calcStatusTrends = isOFF(params, "calcStatusTrends", activeComputations);
        calcFlipFlops = isOFF(params, "calcFlipFlops", activeComputations);
        calcTagTrends = isOFF(params, "calcTagTrends", activeComputations);

        if (!calcStatus && !calcAR && !calcStatusTrends && !calcFlipFlops && !calcStatusTrends) {
            System.exit(0);
        }

    }

    public static boolean isOFF(ParameterTool params, String paramName, ReportManager.ActiveComputations
            activeComputations) {
        if (params.has(paramName)) {
            return !params.get(paramName).equals("OFF");
        } else {
            switch (paramName) {
                case "calcStatus":
                    return activeComputations.isStatus();
                case "calcAR":
                    return activeComputations.isAr();

                case "calcStatusTrends":
                    return activeComputations.isStatusTrends();
                case "calcFlipFlops":
                    return activeComputations.isFlipflops();

                case "calcTagTrends":
                    return activeComputations.isTagTrends();
                default:
                    return false;
            }
        }
    }
}
