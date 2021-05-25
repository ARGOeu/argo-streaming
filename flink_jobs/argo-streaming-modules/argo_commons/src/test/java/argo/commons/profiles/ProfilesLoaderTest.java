package argo.commons.profiles;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import argo.commons.requests.RequestManagerTest;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
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
public class ProfilesLoaderTest {

    public ProfilesLoaderTest() {
    }

    @BeforeClass
    public static void setUpClass() {
     //   assertNotNull("Test file missing", ProfilesLoaderTest.class.getResource("/profiles/config.properties"));
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
     * Test of getReportParser method, of class ProfilesLoader.
     */
    @Test
    public void testGetReportParser() throws IOException, ParseException, java.text.ParseException {
        System.out.println("getReportParser");
        String apiUri = "https://api.devel.argo.grnet.gr/api/v2";
        String key = readPropertiesFile();
        String proxy = null;
        String date = "2021-01-15";
        String reportId = "04edb428-01e6-4286-87f1-050546736f7c";
        if (key != null && !key.equals("")) {
            ProfilesLoader instance = new ProfilesLoader(apiUri, key, proxy, reportId, date);

            ReportParser expResult = new ReportParser(apiUri, key, proxy, reportId);

            ReportParser result = instance.getReportParser();
            assertEquals(expResult, result);
        }
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of setReportParser method, of class ProfilesLoader.
     */
    @Test
    public void testSetReportParser() throws IOException, ParseException, java.text.ParseException {
        System.out.println("setReportParser");

        String apiUri = "https://api.devel.argo.grnet.gr/api/v2";
        String key = readPropertiesFile();
        String proxy = null;
        String date = "2021-01-15";
        String reportId = "04edb428-01e6-4286-87f1-050546736f7c";
        if (key != null && !key.equals("")) {
            ReportParser reportParser = new ReportParser(apiUri, key, proxy, reportId);

            ProfilesLoader instance = new ProfilesLoader(apiUri, key, proxy, reportId, date);

            instance.setReportParser(reportParser);
        }
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getTopologyEndpointParser method, of class ProfilesLoader.
     */
    @Test
    public void testGetTopologyEndpointParser() throws IOException, ParseException, java.text.ParseException {
        System.out.println("getTopologyEndpointParser");
        String apiUri = "https://api.devel.argo.grnet.gr/api/v2";
        String key = readPropertiesFile();
        String proxy = null;
        String date = "2021-01-15";
        String reportname = "SLA";
        String reportId = "04edb428-01e6-4286-87f1-050546736f7c";
        if (key != null && !key.equals("")) {
            TopologyEndpointParser expResult = new TopologyEndpointParser(apiUri, key, proxy, date, reportname);
            ProfilesLoader instance = new ProfilesLoader(apiUri, key, proxy, reportId, date);

            TopologyEndpointParser result = instance.getTopologyEndpointParser();
            assertEquals(expResult, result);
        }
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of setTopologyEndpointParser method, of class ProfilesLoader.
     */
    @Test
    public void testSetTopologyEndpointParser() throws IOException, ParseException, java.text.ParseException, java.text.ParseException {
        System.out.println("setTopologyEndpointParser");
        String apiUri = "https://api.devel.argo.grnet.gr/api/v2";
        String key = readPropertiesFile();
        String proxy = null;
        String date = "2021-01-15";
        String reportname = "SLA";
        String reportId = "04edb428-01e6-4286-87f1-050546736f7c";
        if (key != null && !key.equals("")) {
            TopologyEndpointParser topologyEndpointParser = new TopologyEndpointParser(apiUri, key, proxy, date, reportname);

            ProfilesLoader instance = new ProfilesLoader(apiUri, key, proxy, reportId, date);

            instance.setTopologyEndpointParser(topologyEndpointParser);
        }
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getMetricProfileParser method, of class ProfilesLoader.
     */
    @Test
    public void testGetMetricProfileParser() throws IOException, ParseException, java.text.ParseException {
        System.out.println("getMetricProfileParser");
        String apiUri = "https://api.devel.argo.grnet.gr/api/v2";
        String key = readPropertiesFile();
        String proxy = null;
        String date = "2021-01-15";
        String metricId = "5850d7fe-75f5-4aa4-bacc-9a5c10280b59";
        String reportId = "04edb428-01e6-4286-87f1-050546736f7c";
        if (key != null && !key.equals("")) {
            MetricProfileParser expResult = new MetricProfileParser(apiUri, key, proxy, metricId, date);
            ProfilesLoader instance = new ProfilesLoader(apiUri, key, proxy, reportId, date);

            MetricProfileParser result = instance.getMetricProfileParser();
            assertEquals(expResult, result);
        }
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of setMetricProfileParser method, of class ProfilesLoader.
     */
    @Test
    public void testSetMetricProfileParser() throws IOException, ParseException, java.text.ParseException {
        System.out.println("setMetricProfileParser");

        String apiUri = "https://api.devel.argo.grnet.gr/api/v2";
        String key = readPropertiesFile();
        String proxy = null;
        String date = "2021-01-15";
        String metricId = "5850d7fe-75f5-4aa4-bacc-9a5c10280b59";
        String reportId = "04edb428-01e6-4286-87f1-050546736f7c";
        if (key != null && !key.equals("")) {
            MetricProfileParser metricProfileParser = new MetricProfileParser(apiUri, key, proxy, metricId, date);

            ProfilesLoader instance = new ProfilesLoader(apiUri, key, proxy, reportId, date);

            instance.setMetricProfileParser(metricProfileParser);
        }
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of getOperationParser method, of class ProfilesLoader.
     */
    @Test
    public void testGetOperationParser() throws IOException, ParseException, java.text.ParseException {
        System.out.println("getOperationParser");
        String apiUri = "https://api.devel.argo.grnet.gr/api/v2";
        String key = readPropertiesFile();
        String proxy = null;
        String date = "2021-01-15";
        String url = "/operations_profiles";
        String operationsId = "8ce59c4d-3761-4f25-a364-f019e394bf8b";
        String reportId = "04edb428-01e6-4286-87f1-050546736f7c";
        if (key != null && !key.equals("")) {
            OperationsParser expResult = new OperationsParser(apiUri, key, proxy, operationsId, date);
            ProfilesLoader instance = new ProfilesLoader(apiUri, key, proxy, reportId, date);

            OperationsParser result = instance.getOperationParser();
            assertEquals(expResult, result);
        }
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of setOperationParser method, of class ProfilesLoader.
     */
    @Test
    public void testSetOperationParser() throws IOException, ParseException, java.text.ParseException {
        System.out.println("setOperationParser");
        String apiUri = "https://api.devel.argo.grnet.gr/api/v2";
        String key = readPropertiesFile();
        String proxy = null;
        String date = "2021-01-15";
        String url = "/operations_profiles";
        String operationsId = "8ce59c4d-3761-4f25-a364-f019e394bf8b";
        String reportId = "04edb428-01e6-4286-87f1-050546736f7c";
        if (key != null && !key.equals("")) {
            OperationsParser operationParser = new OperationsParser(apiUri, key, proxy, operationsId, date);
            ProfilesLoader instance = new ProfilesLoader(apiUri, key, proxy, reportId, date);

            instance.setOperationParser(operationParser);
        }
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of getAggregationProfileParser method, of class ProfilesLoader.
     */
    @Test
    public void testGetAggregationProfileParser() throws IOException, ParseException, java.text.ParseException {
        System.out.println("getAggregationProfileParser");
        String apiUri = "https://api.devel.argo.grnet.gr/api/v2";
        String key = readPropertiesFile();
        String proxy = null;
        String date = "2021-01-15";
        String aggregationId = "00272336-7199-4ea4-bbe9-043aca02838c";
        String reportId = "04edb428-01e6-4286-87f1-050546736f7c";
        if (key != null && !key.equals("")) {
            AggregationProfileParser expResult = new AggregationProfileParser(apiUri, key, proxy, aggregationId, date);
            ProfilesLoader instance = new ProfilesLoader(apiUri, key, proxy, reportId, date);

            AggregationProfileParser result = instance.getAggregationProfileParser();
            assertEquals(expResult, result);
        }
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of setAggregationProfileParser method, of class ProfilesLoader.
     */
    @Test
    public void testSetAggregationProfileParser() throws IOException, ParseException, java.text.ParseException {
        System.out.println("setAggregationProfileParser");
        String apiUri = "https://api.devel.argo.grnet.gr/api/v2";
        String key = readPropertiesFile();
        String proxy = null;
        String date = "2021-01-15";
        String aggregationId = "00272336-7199-4ea4-bbe9-043aca02838c";
        String reportId = "04edb428-01e6-4286-87f1-050546736f7c";
        if (key != null && !key.equals("")) {
            AggregationProfileParser aggregationProfileParser = new AggregationProfileParser(apiUri, key, proxy, aggregationId, date);

            ProfilesLoader instance = new ProfilesLoader(apiUri, key, proxy, reportId, date);

            instance.setAggregationProfileParser(aggregationProfileParser);
        }
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of getTopolGroupParser method, of class ProfilesLoader.
     */
    @Test
    public void testGetTopolGroupParser() throws IOException, ParseException, java.text.ParseException {
        System.out.println("getTopolGroupParser");
        String apiUri = "https://api.devel.argo.grnet.gr/api/v2";
        String key = readPropertiesFile();
        String proxy = null;
        String date = "2021-01-15";
        String reportname = "SLA";
        String reportId = "04edb428-01e6-4286-87f1-050546736f7c";
        if (key != null && !key.equals("")) {
            TopologyGroupParser expResult = new TopologyGroupParser(apiUri, key, proxy, date, reportname);
            ProfilesLoader instance = new ProfilesLoader(apiUri, key, proxy, reportId, date);

            TopologyGroupParser result = instance.getTopolGroupParser();
            assertEquals(expResult, result);
        }
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of setTopolGroupParser method, of class ProfilesLoader.
     */
    @Test
    public void testSetTopolGroupParser() throws IOException, ParseException, java.text.ParseException {
        System.out.println("setTopolGroupParser");

        String apiUri = "https://api.devel.argo.grnet.gr/api/v2";
        String key = readPropertiesFile();
        String proxy = null;
        String date = "2021-01-15";
        String reportname = "SLA";
        String reportId = "04edb428-01e6-4286-87f1-050546736f7c";
        if (key != null && !key.equals("")) {

            TopologyGroupParser topolGroupParser = new TopologyGroupParser(apiUri, key, proxy, date, reportname);

            ProfilesLoader instance = new ProfilesLoader(apiUri, key, proxy, reportId, date);
            instance.setTopolGroupParser(topolGroupParser);
        }
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    private String readPropertiesFile() throws FileNotFoundException, IOException, org.json.simple.parser.ParseException {
        String propFileName = "config.properties";
        Properties prop = new Properties();
        InputStream inputStream = RequestManagerTest.class.getResourceAsStream("/profiles/config.properties");

        if (inputStream != null) {
            prop.load(inputStream);
        } else {
            throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
        }

        // get the property value and print it out
        String argoToken = prop.getProperty("argo.webapi.token");
        return argoToken;
    }

}
