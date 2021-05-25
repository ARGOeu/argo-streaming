package argo.commons.profiles;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
/**
 *
 * @author cthermolia
 */
public class ReportParserTest {

    public ReportParserTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        
        assertNotNull("Test file missing", ReportParserTest.class.getResource("/profiles/report.json"));
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
     * Test of loadReportInfo method, of class ReportParser.
     */
//    @Test
//    public void testLoadReportInfo() throws Exception {
//        System.out.println("loadReportInfo");
//
//       JSONObject reportJSONObject = readJsonFromFile(ReportParserTest.class.getResource("/profiles/report.json").getFile());
//        ReportParser instance = new ReportParser(reportJSONObject);
//        instance.loadReportInfo(uri, key, proxy);
//        // TODO review the generated test code and remove the default call to fail.
//        //fail("The test case is a prototype.");
//    }

    /**
     * Test of getAggregationReportId method, of class ReportParser.
     */
    @Test
    public void testGetAggregationReportId() throws IOException, ParseException {
        System.out.println("getAggregationReportId");

        JSONObject reportJSONObject = readJsonFromFile(ReportParserTest.class.getResource("/profiles/report.json").getFile());
        ReportParser instance = new ReportParser(reportJSONObject);
        String expResult = "00272336-7199-4ea4-bbe9-043aca02838c";

        String result = instance.getAggregationReportId();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getMetricReportId method, of class ReportParser.
     */
    @Test
    public void testGetMetricReportId() throws IOException, ParseException {
        System.out.println("getMetricReportId");
        JSONObject reportJSONObject = readJsonFromFile(ReportParserTest.class.getResource("/profiles/report.json").getFile());
        ReportParser instance = new ReportParser(reportJSONObject);
      
        String expResult = "5850d7fe-75f5-4aa4-bacc-9a5c10280b59";
        String result = instance.getMetricReportId();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getOperationReportId method, of class ReportParser.
     */
    @Test
    public void testGetOperationReportId() throws IOException, ParseException {
        System.out.println("getOperationReportId");
        JSONObject reportJSONObject = readJsonFromFile(ReportParserTest.class.getResource("/profiles/report.json").getFile());
        ReportParser instance = new ReportParser(reportJSONObject);
      
        String expResult = "8ce59c4d-3761-4f25-a364-f019e394bf8b";
        String result = instance.getOperationReportId();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //   fail("The test case is a prototype.");
    }

    /**
     * Test of getTenantReport method, of class ReportParser.
     */
    @Test
    public void testGetTenantReport() throws IOException, ParseException {
        System.out.println("getTenantReport");
        //String reportId = "04edb428-01e6-4286-87f1-050546736f7c";
         JSONObject reportJSONObject = readJsonFromFile(ReportParserTest.class.getResource("/profiles/report.json").getFile());
        ReportParser instance = new ReportParser(reportJSONObject);
      
        ReportParser.Threshold threshold = instance.new Threshold(Long.valueOf(80), Long.valueOf(85), 0.8, 0.1, 0.1);
        ReportParser.Topology topology = instance.new Topology("PROJECT", instance.new Topology("SERVICEGROUPS", null));
        ArrayList<ReportParser.Profiles> profiles = new ArrayList<>();
        profiles.add(instance.new Profiles("5850d7fe-75f5-4aa4-bacc-9a5c10280b59", "ARGO_MON_CRITICAL", "metric"));
        profiles.add(instance.new Profiles("00272336-7199-4ea4-bbe9-043aca02838c", "sla_test", "aggregation"));
        profiles.add(instance.new Profiles("8ce59c4d-3761-4f25-a364-f019e394bf8b", "egi_ops", "operations"));
        ArrayList<ReportParser.FilterTags> filterTags = new ArrayList<>();
        filterTags.add(instance.new FilterTags("production", "1", "argo.endpoints.filter.tags"));
        filterTags.add(instance.new FilterTags("monitored", "1", "argo.endpoints.filter.tags"));
        filterTags.add(instance.new FilterTags("scope", "*SLA", "argo.group.filter.tags"));
        String[] info = {"SLA", "A/R report based on mapping SLAs to EGI service groups", "2018-09-28 15:08:48", "2021-05-21 11:24:48"};
        ReportParser.TenantReport tenantReport = instance.new TenantReport("04edb428-01e6-4286-87f1-050546736f7c", "EGI", false, info, topology, threshold, profiles, filterTags);

        
        ReportParser.TenantReport expResult = tenantReport;
        ReportParser.TenantReport result = instance.getTenantReport();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }
   private JSONObject readJsonFromFile(String path) throws FileNotFoundException, IOException, org.json.simple.parser.ParseException {
        JSONParser parser = new JSONParser();
      
        Object obj = parser.parse(new FileReader(path));
        JSONObject jsonObject = (JSONObject) obj;

        return jsonObject;
    }

}
