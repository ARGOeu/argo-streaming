package profilesmanager;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class RecomputationManagerTest {
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        // Assert that files are present
        assertNotNull("Test file missing", RecomputationManagerTest.class.getResource("/profiles/recomp.json"));
    }

    @Test
    public void test() throws URISyntaxException, ParseException, IOException {
        // Prepare Resource File
        URL resJsonFile = RecomputationManagerTest.class.getResource("/profiles/recomp.json");
        File jsonFile = new File(resJsonFile.toURI());

        RecomputationsManager.loadJson(jsonFile);

        assertEquals(RecomputationsManager.isExcluded("GR-01-AUTH"), true);
        assertEquals(RecomputationsManager.isExcluded("HG-03-AUTH"), true);
        assertEquals(RecomputationsManager.isExcluded("GR-04-IASA"), false);

        // Check period functionality
        ArrayList<Map<String, String>> gr01list = new ArrayList<Map<String, String>>();
        ArrayList<Map<String, String>> siteAlist = new ArrayList<Map<String, String>>();
        ArrayList<Map<String, String>> siteBlist = new ArrayList<Map<String, String>>();
        ArrayList<Map<String, String>> siteClist = new ArrayList<Map<String, String>>();

        Map<String, String> gr01map = new HashMap<String, String>();

        Map<String, String> siteA1map = new HashMap<String, String>();
        Map<String, String> siteA2map = new HashMap<String, String>();

        Map<String, String> siteBmap = new HashMap<String, String>();
        Map<String, String> siteCmap = new HashMap<String, String>();

        // Check period functionality
        gr01map.put("start", "2013-12-08T12:03:44Z");
        gr01map.put("end", "2013-12-10T12:03:44Z");

        siteA1map.put("start", "2013-12-08T12:03:44Z");
        siteA1map.put("end", "2013-12-08T13:03:44Z");

        siteA2map.put("start", "2013-12-08T16:03:44Z");
        siteA2map.put("end", "2013-12-08T18:03:44Z");

        siteBmap.put("start", "2013-12-08T12:03:44Z");
        siteBmap.put("end", "2013-12-08T13:03:44Z");

        siteCmap.put("start", "2013-12-08T16:03:44Z");
        siteCmap.put("end", "2013-12-08T18:03:44Z");

        gr01list.add(gr01map);
        siteAlist.add(siteA1map);
        siteAlist.add(siteA2map);
        siteBlist.add(siteBmap);
        siteClist.add(siteCmap);

        Assert.assertEquals(RecomputationsManager.getPeriods("GR-01-AUTH", "2013-12-08"), gr01list);
        Assert.assertEquals(RecomputationsManager.getPeriods("SITE-A", "2013-12-08"), siteAlist);
        Assert.assertEquals(RecomputationsManager.getPeriods("SITE-B", "2013-12-08"), siteBlist);

        // check monitoring exclusions
        Assert.assertEquals(false, RecomputationsManager.isMonExcluded("monA", "2013-12-08T11:03:43Z"));
        Assert.assertEquals(false, RecomputationsManager.isMonExcluded("monA", "2013-12-08T11:03:44Z"));
        Assert.assertEquals(true, RecomputationsManager.isMonExcluded("monA", "2013-12-08T12:06:44Z"));
        Assert.assertEquals(true, RecomputationsManager.isMonExcluded("monA", "2013-12-08T14:05:44Z"));
        Assert.assertEquals(true, RecomputationsManager.isMonExcluded("monA", "2013-12-08T15:02:44Z"));
        Assert.assertEquals(false, RecomputationsManager.isMonExcluded("monA", "2013-12-08T15:03:45Z"));

        // check monitoring exclusions
        Assert.assertEquals(false, RecomputationsManager.isMonExcluded("monB", "2013-12-08T11:03:43Z"));
        Assert.assertEquals(false, RecomputationsManager.isMonExcluded("monB", "2013-12-08T11:03:44Z"));
        Assert.assertEquals(false, RecomputationsManager.isMonExcluded("monB", "2013-12-08T12:06:44Z"));
        Assert.assertEquals(false, RecomputationsManager.isMonExcluded("monB", "2013-12-08T14:05:44Z"));
        Assert.assertEquals(false, RecomputationsManager.isMonExcluded("monB", "2013-12-08T15:02:44Z"));
        Assert.assertEquals(false, RecomputationsManager.isMonExcluded("monB", "2013-12-08T15:03:45Z"));
        RecomputationsManager.RecomputationElement exMetric1 = new RecomputationsManager.RecomputationElement(null, null, null, "metric1", "2013-12-08T12:03:44Z", "2013-12-08T13:03:44Z", "EXCLUDED", RecomputationsManager.ElementType.METRIC);
        RecomputationsManager.RecomputationElement exMetric2 = new RecomputationsManager.RecomputationElement(null, null, "host2.example.com", "metric2", "2013-12-08T12:03:44Z", "2013-12-08T13:03:44Z", "EXCLUDED", RecomputationsManager.ElementType.METRIC);
        RecomputationsManager.RecomputationElement exMetric3 = new RecomputationsManager.RecomputationElement("grnet", null, null, "metric2", "2013-12-08T12:03:44Z", "2013-12-08T13:03:44Z", "EXCLUDED", RecomputationsManager.ElementType.METRIC);
        RecomputationsManager.RecomputationElement exMetric4 = new RecomputationsManager.RecomputationElement("grnet", "cloud.storage", null, "webcheck", "2013-12-08T12:03:44Z", "2013-12-08T13:03:44Z", "EXCLUDED", RecomputationsManager.ElementType.METRIC);
        List<RecomputationsManager.RecomputationElement> exMetrics = new ArrayList<>();
        HashMap<String, List> expMetricsMap = new HashMap<>();
        List<RecomputationsManager.RecomputationElement> exMetrics1 = new ArrayList<>();


        List<RecomputationsManager.RecomputationElement> exMetrics1List = new ArrayList<>();
        exMetrics1List.add(exMetric1);
        expMetricsMap.put(exMetric1.getMetric(), exMetrics1List);

        List<RecomputationsManager.RecomputationElement> exMetrics2List = new ArrayList<>();
        exMetrics2List.add(exMetric2);
        expMetricsMap.put(exMetric2.getMetric(), exMetrics2List);


        exMetrics2List.add(exMetric3);
        expMetricsMap.put(exMetric3.getMetric(), exMetrics2List);

        List<RecomputationsManager.RecomputationElement> exMetrics4List = new ArrayList<>();
        exMetrics4List.add(exMetric4);
        expMetricsMap.put(exMetric4.getMetric(), exMetrics4List);


        Assert.assertEquals(expMetricsMap, RecomputationsManager.metricRecomputationItems);

        Assert.assertEquals(exMetric1, RecomputationsManager.findMetricExcluded(null, null, null, "metric1"));
        Assert.assertEquals(exMetric2, RecomputationsManager.findMetricExcluded(null, null, "host2.example.com", "metric2"));
        Assert.assertEquals(exMetric3, RecomputationsManager.findMetricExcluded("grnet", null, null, "metric2"));
        Assert.assertEquals(exMetric4, RecomputationsManager.findMetricExcluded("grnet", "cloud.storage", null, "webcheck"));
        Assert.assertEquals(null, RecomputationsManager.findMetricExcluded("grnet", null, null, "webcheck"));


    }

}