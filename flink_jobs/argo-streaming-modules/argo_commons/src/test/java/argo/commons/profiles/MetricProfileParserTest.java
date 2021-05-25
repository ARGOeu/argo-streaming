package argo.commons.profiles;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import argo.commons.timelines.TimelineMergerTest;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author cthermolia
 */
public class MetricProfileParserTest {

    public MetricProfileParserTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        assertNotNull("Test file missing", MetricProfileParserTest.class.getResource("/profiles/metricprofile.json"));
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of containsMetric method, of class MetricProfileParser.
     */
    @Test
    public void testContainsMetric() throws IOException, ParseException {
        System.out.println("containsMetric");
        String service = "GRAM5";
        String metric = "eu.egi.GRAM-CertValidity";
        JSONObject mprofileJSONObject = readJsonFromFile(MetricProfileParserTest.class.getResource("/profiles/metricprofile.json").getFile());
        MetricProfileParser instance = new MetricProfileParser(mprofileJSONObject);

        boolean expResult = true;
        boolean result = instance.containsMetric(service, metric);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getId method, of class MetricProfileParser.
     */
    @Test
    public void testGetId() throws IOException, ParseException {
        System.out.println("getId");
        JSONObject mprofileJSONObject = readJsonFromFile(MetricProfileParserTest.class.getResource("/profiles/metricprofile.json").getFile());
        MetricProfileParser instance = new MetricProfileParser(mprofileJSONObject);

        String expResult = "5850d7fe-75f5-4aa4-bacc-9a5c10280b59";
        String result = instance.getId();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getDate method, of class MetricProfileParser.
     */
    @Test
    public void testGetDate() throws IOException, ParseException {
        System.out.println("getDate");
        JSONObject mprofileJSONObject = readJsonFromFile(MetricProfileParserTest.class.getResource("/profiles/metricprofile.json").getFile());
        MetricProfileParser instance = new MetricProfileParser(mprofileJSONObject);

        String expResult = "2021-03-01";
        String result = instance.getDate();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //    fail("The test case is a prototype.");
    }

    /**
     * Test of getName method, of class MetricProfileParser.
     */
    @Test
    public void testGetName() throws IOException, ParseException {
        System.out.println("getName");
        JSONObject mprofileJSONObject = readJsonFromFile(MetricProfileParserTest.class.getResource("/profiles/metricprofile.json").getFile());
        MetricProfileParser instance = new MetricProfileParser(mprofileJSONObject);

        String expResult = "ARGO_MON_CRITICAL";
        String result = instance.getName();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of getDescription method, of class MetricProfileParser.
     */
    @Test
    public void testGetDescription() throws IOException, ParseException {
        System.out.println("getDescription");
        JSONObject mprofileJSONObject = readJsonFromFile(MetricProfileParserTest.class.getResource("/profiles/metricprofile.json").getFile());
        MetricProfileParser instance = new MetricProfileParser(mprofileJSONObject);

        String expResult = "Central ARGO-MON_CRITICAL profile";
        String result = instance.getDescription();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getServices method, of class MetricProfileParser.
     */
    @Test
    public void testGetServices() throws IOException, ParseException {
        System.out.println("getServices");
        JSONObject mprofileJSONObject = readJsonFromFile(MetricProfileParserTest.class.getResource("/profiles/metricprofile.json").getFile());
        MetricProfileParser instance = new MetricProfileParser(mprofileJSONObject);
        ArrayList<MetricProfileParser.Services> expResult = new ArrayList<>();

        MetricProfileParser.Services serv1 = instance.new Services("ARC-CE",
                new ArrayList<>(Arrays.asList(
                        "org.nordugrid.ARC-CE-ARIS",
                        "org.nordugrid.ARC-CE-IGTF",
                        "org.nordugrid.ARC-CE-result",
                        "org.nordugrid.ARC-CE-srm"
                )));

        MetricProfileParser.Services serv2 = instance.new Services("GRAM5",
                new ArrayList<>(Arrays.asList(
                        "eu.egi.GRAM-CertValidity",
                        "hr.srce.GRAM-Auth",
                        "hr.srce.GRAM-Command"
                )));

        MetricProfileParser.Services serv3 = instance.new Services("org.opensciencegrid.htcondorce",
                new ArrayList<>(Arrays.asList(
                        "ch.cern.HTCondorCE-JobState",
                        "ch.cern.HTCondorCE-JobSubmit"
                )));

        MetricProfileParser.Services serv4 = instance.new Services("org.openstack.nova",
                new ArrayList<>(Arrays.asList(
                        "eu.egi.cloud.OpenStack-VM",
                        "org.nagios.Keystone-TCP"
                )));

        MetricProfileParser.Services serv5 = instance.new Services("QCG.Computing",
                new ArrayList<>(Arrays.asList(
                        "eu.egi.QCG-Computing-CertValidity",
                        "pl.plgrid.QCG-Computing"
                )));

        MetricProfileParser.Services serv6 = instance.new Services("SRM",
                new ArrayList<>(Arrays.asList(
                        "eu.egi.SRM-CertValidity",
                        "eu.egi.SRM-GetSURLs",
                        "eu.egi.SRM-VODel",
                        "eu.egi.SRM-VOGet",
                        "eu.egi.SRM-VOGetTurl",
                        "eu.egi.SRM-VOLs",
                        "eu.egi.SRM-VOLsDir",
                        "eu.egi.SRM-VOPut"
                )));

        MetricProfileParser.Services serv7 = instance.new Services("webdav",
                new ArrayList<>(Arrays.asList(
                        "ch.cern.WebDAV"
                )));
        expResult.add(serv1);
        expResult.add(serv2);
        expResult.add(serv3);
        expResult.add(serv4);
        expResult.add(serv5);
        expResult.add(serv6);
        expResult.add(serv7);

        ArrayList<MetricProfileParser.Services> result = instance.getServices();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getMetricData method, of class MetricProfileParser.
     */
    @Test
    public void testGetMetricData() throws IOException, ParseException {
        System.out.println("getMetricData");
        JSONObject mprofileJSONObject = readJsonFromFile(MetricProfileParserTest.class.getResource("/profiles/metricprofile.json").getFile());
        MetricProfileParser instance = new MetricProfileParser(mprofileJSONObject);

        HashMap<String, ArrayList<String>> expResult = new HashMap<>();

        expResult.put("ARC-CE", new ArrayList<>(Arrays.asList(
                "org.nordugrid.ARC-CE-ARIS",
                "org.nordugrid.ARC-CE-IGTF",
                "org.nordugrid.ARC-CE-result",
                "org.nordugrid.ARC-CE-srm"
        )));

        expResult.put("GRAM5", new ArrayList<>(Arrays.asList(
                "eu.egi.GRAM-CertValidity",
                "hr.srce.GRAM-Auth",
                "hr.srce.GRAM-Command"
        )));

        expResult.put("org.opensciencegrid.htcondorce", new ArrayList<>(Arrays.asList(
                "ch.cern.HTCondorCE-JobState",
                "ch.cern.HTCondorCE-JobSubmit"
        )));

        expResult.put("org.openstack.nova", new ArrayList<>(Arrays.asList(
                "eu.egi.cloud.OpenStack-VM",
                "org.nagios.Keystone-TCP"
        )));

        expResult.put("QCG.Computing", new ArrayList<>(Arrays.asList(
                "eu.egi.QCG-Computing-CertValidity",
                "pl.plgrid.QCG-Computing"
        )));

        expResult.put("SRM", new ArrayList<>(Arrays.asList(
                "eu.egi.SRM-CertValidity",
                "eu.egi.SRM-GetSURLs",
                "eu.egi.SRM-VODel",
                "eu.egi.SRM-VOGet",
                "eu.egi.SRM-VOGetTurl",
                "eu.egi.SRM-VOLs",
                "eu.egi.SRM-VOLsDir",
                "eu.egi.SRM-VOPut"
        )));

        expResult.put("webdav", new ArrayList<>(Arrays.asList(
                "ch.cern.WebDAV"
        )));

        HashMap<String, ArrayList<String>> result = instance.getMetricData();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    private JSONObject readJsonFromFile(String path) throws FileNotFoundException, IOException, org.json.simple.parser.ParseException {
        JSONParser parser = new JSONParser();
        Object obj = parser.parse(new FileReader(path));

        JSONObject jsonObject = (JSONObject) obj;

        return jsonObject;
    }

}
