package argo.amr;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.github.tomakehurst.wiremock.junit.WireMockRule;

import argo.avro.Downtime;
import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;
import argo.avro.MetricProfile;
import argo.avro.Weight;

import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

public class ApiResourceManagerTest {

    public static String loadResJSON(String resURL) {

        InputStream jsonInputStream
                = ApiResourceManagerTest.class.getResourceAsStream(resURL);
        String content = new BufferedReader(
                new InputStreamReader(jsonInputStream, StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n"));
        return content;

    }

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().httpsPort(8443));

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        // Assert that files are present
        assertNotNull("Test file missing", ApiResourceManagerTest.class.getResource("/amr/report.json"));
        assertNotNull("Test file missing", ApiResourceManagerTest.class.getResource("/amr/report.json"));
        assertNotNull("Test file missing", ApiResourceManagerTest.class.getResource("/amr/metric_profile.json"));
        assertNotNull("Test file missing", ApiResourceManagerTest.class.getResource("/amr/agg_profile.json"));
        assertNotNull("Test file missing", ApiResourceManagerTest.class.getResource("/amr/ops_profile.json"));
        assertNotNull("Test file missing", ApiResourceManagerTest.class.getResource("/amr/thresholds.json"));
        assertNotNull("Test file missing", ApiResourceManagerTest.class.getResource("/amr/topoendpoints.json"));
        assertNotNull("Test file missing", ApiResourceManagerTest.class.getResource("/amr/topogroups.json"));
        assertNotNull("Test file missing", ApiResourceManagerTest.class.getResource("/amr/downtimes.json"));
        assertNotNull("Test file missing", ApiResourceManagerTest.class.getResource("/amr/weights.json"));
        assertNotNull("Test file missing", ApiResourceManagerTest.class.getResource("/amr/recomputations.json"));
        assertNotNull("Test file missing", ApiResourceManagerTest.class.getResource("/amr/data_CONFIG.json"));
        assertNotNull("Test file missing", ApiResourceManagerTest.class.getResource("/amr/data_METRIC.json"));
        assertNotNull("Test file missing", ApiResourceManagerTest.class.getResource("/amr/data_AGGREGATION.json"));
        assertNotNull("Test file missing", ApiResourceManagerTest.class.getResource("/amr/data_OPS.json"));
        assertNotNull("Test file missing", ApiResourceManagerTest.class.getResource("/amr/data_THRESHOLDS.json"));
        assertNotNull("Test file missing", ApiResourceManagerTest.class.getResource("/amr/data_TOPOENDPOINTS.json"));
        assertNotNull("Test file missing", ApiResourceManagerTest.class.getResource("/amr/data_TOPOGROUPS.json"));
        assertNotNull("Test file missing", ApiResourceManagerTest.class.getResource("/amr/data_DOWNTIMES.json"));
        assertNotNull("Test file missing", ApiResourceManagerTest.class.getResource("/amr/data_WEIGHTS.json"));
        assertNotNull("Test file missing", ApiResourceManagerTest.class.getResource("/amr/data_RECOMPUTATIONS.json"));
        assertNotNull("Test file missing", ApiResourceManagerTest.class.getResource("/amr/metric_tags.json"));
    }

    @Test
    public void test() throws URISyntaxException, IOException, ParseException {
        // load mock api response content
        String jsonReport = loadResJSON("/amr/report.json");
        String jsonMetric = loadResJSON("/amr/metric_profile.json");
        String jsonAgg = loadResJSON("/amr/agg_profile.json");
        String jsonOps = loadResJSON("/amr/ops_profile.json");
        String jsonThresholds = loadResJSON("/amr/thresholds.json");
        String jsonTopoEnd = loadResJSON("/amr/topoendpoints.json");
        String jsonTopoGroups = loadResJSON("/amr/topogroups.json");
        String jsonDowntimes = loadResJSON("/amr/downtimes.json");
        String jsonWeights = loadResJSON("/amr/weights.json");
        String jsonRecomp = loadResJSON("/amr/recomputations.json");
//        String jsonMetricTags = loadResJSON("/amr/metric_tags.json");
        String jsonMetricTags = "{}";

        // get json data items
        String dataConfig = loadResJSON("/amr/data_CONFIG.json");
        String dataMetric = loadResJSON("/amr/data_METRIC.json");
        String dataAggr = loadResJSON("/amr/data_AGGREGATION.json");
        String dataOps = loadResJSON("/amr/data_OPS.json");
        String dataThresh = loadResJSON("/amr/data_THRESHOLDS.json");
        String dataTopoEnd = loadResJSON("/amr/data_TOPOENDPOINTS.json");
        String dataTopoGroup = loadResJSON("/amr/data_TOPOGROUPS.json");
        String dataDown = loadResJSON("/amr/data_DOWNTIMES.json");
        String dataWeights = loadResJSON("/amr/data_WEIGHTS.json");
        String dataRecomp = loadResJSON("/amr/data_RECOMPUTATIONS.json");
        // String dataMetricTags = loadResJSON("/amr/data_RECOMPUTATIONS.json");
        String dataMetricTags = null;
        stubFor(get(urlEqualTo("/api/v2/reports/f29eeb59-ab38-4aa0-b372-5d3c0709dfb2"))
                .willReturn(aResponse().withBody(jsonReport)));
        stubFor(get(urlEqualTo("/api/v2/metric_profiles/92fa5d74-015c-4122-b8b9-7b344f3154d4?date=2020-11-01"))
                .willReturn(aResponse().withBody(jsonMetric)));
        stubFor(get(urlEqualTo("/api/v2/aggregation_profiles/2744247f-40f8-4dd6-b22c-76a3b38334d8?date=2020-11-01"))
                .willReturn(aResponse().withBody(jsonAgg)));
        stubFor(get(urlEqualTo("/api/v2/operations_profiles/ea62ff1e-c6e1-438b-83c7-9262b3a4f179?date=2020-11-01"))
                .willReturn(aResponse().withBody(jsonOps)));
        stubFor(get(urlEqualTo("/api/v2/thresholds_profiles/3345c3c1-322a-47f1-982c-1d9df1fc065e?date=2020-11-01"))
                .willReturn(aResponse().withBody(jsonThresholds)));
        stubFor(get(urlEqualTo("/api/v2/topology/endpoints/by_report/Critical?date=2020-11-01"))
                .willReturn(aResponse().withBody(jsonTopoEnd)));
        stubFor(get(urlEqualTo("/api/v2/topology/groups/by_report/Critical?date=2020-11-01"))
                .willReturn(aResponse().withBody(jsonTopoGroups)));
        stubFor(get(urlEqualTo("/api/v2/downtimes?date=2020-11-01"))
                .willReturn(aResponse().withBody(jsonDowntimes)));
        stubFor(get(urlEqualTo("/api/v2/weights/3b9602ed-49ec-42f3-8df7-7c35331ebf69?date=2020-11-01"))
                .willReturn(aResponse().withBody(jsonWeights)));
        stubFor(get(urlEqualTo("/api/v2/recomputations?date=2020-11-01"))
                .willReturn(aResponse().withBody(jsonRecomp)));
        stubFor(get(urlEqualTo("/api/v2/metrics/by_report/Critical"))
                .willReturn(aResponse().withBody(jsonMetricTags)));
        ApiResourceManager amr = new ApiResourceManager("localhost:8443", "s3cr3t");
        amr.setDate("2020-11-01");
        amr.setReportID("f29eeb59-ab38-4aa0-b372-5d3c0709dfb2");
        amr.setToken("s3cr3t");
        amr.setWeightsID("3b9602ed-49ec-42f3-8df7-7c35331ebf69");
        amr.setVerify(false);

        // Get the report configuration first and parse it
        amr.getRemoteConfig();
        amr.parseReport();

        assertEquals("report name retrieved", "Critical", amr.getReportName());
        assertEquals("metric id retrieved", "92fa5d74-015c-4122-b8b9-7b344f3154d4", amr.getMetricID());
        assertEquals("ops id retrieved", "ea62ff1e-c6e1-438b-83c7-9262b3a4f179", amr.getOpsID());
        assertEquals("aggregations id retrieved", "2744247f-40f8-4dd6-b22c-76a3b38334d8", amr.getAggregationID());
        assertEquals("thresholds id retrieved", "3345c3c1-322a-47f1-982c-1d9df1fc065e", amr.getThresholdsID());

        assertEquals("retrieved config data", dataConfig, amr.getResourceJSON(ApiResource.CONFIG));

        // get the profiles metric, aggregation, ops and thresholds
        amr.getRemoteMetric();
        amr.getRemoteAggregation();
        amr.getRemoteOps();
        amr.getRemoteThresholds();
        amr.getRemoteMetricTags();

        assertEquals("retrieved metric profile data", dataMetric, amr.getResourceJSON(ApiResource.METRIC));
        assertEquals("retrieved aggregation profile data", dataAggr, amr.getResourceJSON(ApiResource.AGGREGATION));
        assertEquals("retrieved ops profile data", dataOps, amr.getResourceJSON(ApiResource.OPS));
        assertEquals("retrieved thresholds profile data", dataThresh, amr.getResourceJSON(ApiResource.THRESHOLDS));
        assertEquals("retrieved metric tag profile data", dataMetricTags, amr.getResourceJSON(ApiResource.MTAGS));

        // get remote topology
        amr.getRemoteTopoEndpoints();
        amr.getRemoteTopoGroups();

        assertEquals("retrieved topology endpoints", dataTopoEnd, amr.getResourceJSON(ApiResource.TOPOENDPOINTS));
        assertEquals("retrieved topology groups", dataTopoGroup, amr.getResourceJSON(ApiResource.TOPOGROUPS));

        // get remote downtimes
        amr.getRemoteDowntimes();
        assertEquals("retrieved downtimes", dataDown, amr.getResourceJSON(ApiResource.DOWNTIMES));

        // get weights
        amr.getRemoteWeights();
        assertEquals("retrieved downtimes", dataWeights, amr.getResourceJSON(ApiResource.WEIGHTS));

        // get recomputations
        amr.getRemoteRecomputations();
        assertEquals("retrieved recomputations", dataRecomp, amr.getResourceJSON(ApiResource.RECOMPUTATIONS));

        // initate a second amr and check getRemoteAll routine
        ApiResourceManager amr2 = new ApiResourceManager("localhost:8443", "s3cr3t");
        amr2.setDate("2020-11-01");
        amr2.setReportID("f29eeb59-ab38-4aa0-b372-5d3c0709dfb2");
        amr2.setToken("s3cr3t");
        amr2.setWeightsID("3b9602ed-49ec-42f3-8df7-7c35331ebf69");
        amr2.setVerify(false);

        amr2.getRemoteAll();
        // test amr2 downtime list
        Downtime[] dtl = amr2.getListDowntimes();
        assertEquals("downtime list size", 3, dtl.length);
        assertEquals("downtime data", "WebPortal", dtl[0].getService());
        assertEquals("downtime data", "hostA.foo", dtl[0].getHostname());
        assertEquals("downtime data", "2020-11-10T00:00:00Z", dtl[0].getStartTime());
        assertEquals("downtime data", "2020-11-10T23:59:00Z", dtl[0].getEndTime());
        assertEquals("downtime data", "WebPortal", dtl[1].getService());
        assertEquals("downtime data", "hostB.foo", dtl[1].getHostname());
        assertEquals("downtime data", "2020-11-10T00:00:00Z", dtl[1].getStartTime());
        assertEquals("downtime data", "2020-11-10T23:59:00Z", dtl[1].getEndTime());
        assertEquals("downtime data", "WebPortald", dtl[2].getService());
        assertEquals("downtime data", "hostB.foo", dtl[2].getHostname());
        assertEquals("downtime data", "2020-11-10T00:00:00Z", dtl[2].getStartTime());
        assertEquals("downtime data", "2020-11-10T23:59:00Z", dtl[2].getEndTime());

        // test amr2 group endpoint list
        GroupEndpoint[] gel = amr2.getListGroupEndpoints();
        assertEquals("group endpoint list size", 3, gel.length);
        assertEquals("group endpoint data", "SERVICEGROUPS", gel[0].getType());
        assertEquals("group endpoint data", "groupA", gel[0].getGroup());
        assertEquals("group endpoint data", "webPortal", gel[0].getService());
        assertEquals("group endpoint data", "host1.foo.bar", gel[0].getHostname());
        assertEquals("group endpoint data", "1", gel[0].getTags().get("monitored"));
        assertEquals("group endpoint data", "1", gel[0].getTags().get("production"));
        assertEquals("group endpoint data", "FOO", gel[0].getTags().get("scope"));

        assertEquals("group endpoint data", "SERVICEGROUPS", gel[1].getType());
        assertEquals("group endpoint data", "groupB", gel[1].getGroup());
        assertEquals("group endpoint data", "webPortal", gel[1].getService());
        assertEquals("group endpoint data", "host3.foo.bar", gel[1].getHostname());
        assertEquals("group endpoint data", "1", gel[1].getTags().get("monitored"));
        assertEquals("group endpoint data", "1", gel[1].getTags().get("production"));
        assertEquals("group endpoint data", "FOO", gel[1].getTags().get("scope"));

        assertEquals("group endpoint data", "SERVICEGROUPS", gel[2].getType());
        assertEquals("group endpoint data", "groupA", gel[2].getGroup());
        assertEquals("group endpoint data", "webPortal", gel[2].getService());
        assertEquals("group endpoint data", "host2.foo.bar", gel[2].getHostname());
        assertEquals("group endpoint data", "1", gel[2].getTags().get("monitored"));
        assertEquals("group endpoint data", "1", gel[2].getTags().get("production"));
        assertEquals("group endpoint data", "FOO", gel[2].getTags().get("scope"));

        // test amr2 group groups list
        GroupGroup[] ggl = amr2.getListGroupGroups();
        assertEquals("group endpoint list size", 2, ggl.length);
        assertEquals("group endpoint data", "PROJECT", ggl[0].getType());
        assertEquals("group endpoint data", "ORG-A", ggl[0].getGroup());
        assertEquals("group endpoint data", "GROUP-101", ggl[0].getSubgroup());
        assertEquals("group endpoint data", "0", ggl[0].getTags().get("monitored"));
        assertEquals("group endpoint data", "Local", ggl[0].getTags().get("scope"));

        assertEquals("group endpoint data", "PROJECT", ggl[1].getType());
        assertEquals("group endpoint data", "ORG-A", ggl[1].getGroup());
        assertEquals("group endpoint data", "GROUP-202", ggl[1].getSubgroup());
        assertEquals("group endpoint data", "1", ggl[1].getTags().get("monitored"));
        assertEquals("group endpoint data", "Local", ggl[1].getTags().get("scope"));

        // test amr2 weights list
        Weight[] wl = amr2.getListWeights();
        assertEquals("group endpoint list size", 4, wl.length);
        assertEquals("group endpoint data", "computationpower", wl[0].getType());
        assertEquals("group endpoint data", "GROUP-A", wl[0].getSite());
        assertEquals("group endpoint data", "366", wl[0].getWeight());

        assertEquals("group endpoint data", "computationpower", wl[1].getType());
        assertEquals("group endpoint data", "GROUP-B", wl[1].getSite());
        assertEquals("group endpoint data", "4000", wl[1].getWeight());

        assertEquals("group endpoint data", "computationpower", wl[2].getType());
        assertEquals("group endpoint data", "GROUP-C", wl[2].getSite());
        assertEquals("group endpoint data", "19838", wl[2].getWeight());

        assertEquals("group endpoint data", "computationpower", wl[3].getType());
        assertEquals("group endpoint data", "GROUP-D", wl[3].getSite());
        assertEquals("group endpoint data", "19838", wl[3].getWeight());

        // test amr2 metric profile list
        MetricProfile[] mpl = amr2.getListMetrics();
        assertEquals("group endpoint list size", 1, mpl.length);
        assertEquals("group endpoint data", "test-mon", mpl[0].getProfile());
        assertEquals("group endpoint data", "WebPortal", mpl[0].getService());
        assertEquals("group endpoint data", "org.nagios.WebCheck", mpl[0].getMetric());
        assertEquals("group endpoint data", 0, mpl[0].getTags().size());

    }

}
