/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package profilesmanager;

import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import static junit.framework.Assert.assertNotNull;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 */
public class ReportManagerTest {

    public ReportManagerTest() {
        assertNotNull("Test file missing", ReportManagerTest.class.getResource("/profiles/report.json"));
    }

    @BeforeClass
    public static void setUpClass() {
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
     * Test of clear method, of class ReportManager.
     */
    @Test
    public void testClear() throws IOException {
        System.out.println("clear");
        ReportManager instance = new ReportManager();
        //  JsonElement jsonElement = loadJson(new File(ReportManager.class.getResource("/profiles/report.json").getFile()));
        instance.loadJson(new File(ReportManagerTest.class.getResource("/profiles/report.json").getFile()));
        instance.clear();
        // TODO review the generated test code and remove the default call to fail.
        //     fail("The test case is a prototype.");
    }

    /**
     * Test of getReportID method, of class ReportManager.
     */
    @Test
    public void testGetReportID() throws IOException {
        System.out.println("getReportID");
        ReportManager instance = new ReportManager();
        instance.loadJson(new File(ReportManagerTest.class.getResource("/profiles/report.json").getFile()));

        String expResult = "04edb428-01e6-4286-87f1-050546736f7c";
        String result = instance.getReportID();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getReport method, of class ReportManager.
     */
    @Test
    public void testGetReport() throws IOException {
        System.out.println("getReport");
        ReportManager instance = new ReportManager();
        instance.loadJson(new File(ReportManagerTest.class.getResource("/profiles/report.json").getFile()));

        String expResult = "SLA";
        String result = instance.getReport();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getTenant method, of class ReportManager.
     */
    @Test
    public void testGetTenant() throws IOException {
        System.out.println("getTenant");
        ReportManager instance = new ReportManager();
        instance.loadJson(new File(ReportManagerTest.class.getResource("/profiles/report.json").getFile()));

        String expResult = "EGI";
        String result = instance.getTenant();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getEgroup method, of class ReportManager.
     */
    @Test
    public void testGetEgroup() throws IOException {
        System.out.println("getEgroup");
        ReportManager instance = new ReportManager();
        instance.loadJson(new File(ReportManagerTest.class.getResource("/profiles/report.json").getFile()));

        String expResult = "SERVICEGROUPS";
        String result = instance.getEgroup();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of loadJson method, of class ReportManager.
     */
    @Test
    public void testLoadJson() throws Exception {
        System.out.println("loadJson");
        ReportManager instance = new ReportManager();
        instance.loadJson(new File(ReportManagerTest.class.getResource("/profiles/report.json").getFile()));

        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of loadJsonString method, of class ReportManager.
     */
//    @Test
//    public void testLoadJsonString() {
//        System.out.println("loadJsonString");
//        List<String> confJson = null;
//        ReportManager instance = new ReportManager();
//        instance.loadJsonString(confJson);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
    /**
     * Test of readJson method, of class ReportManager.
     */
    @Test
    public void testReadJson() throws IOException {
        System.out.println("readJson");
        JsonElement jElement = loadJson(new File(ReportManagerTest.class.getResource("/profiles/report.json").getFile()));
        ReportManager instance = new ReportManager();
        instance.readJson(jElement);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getAggregationReportId method, of class ReportManager.
     */
    @Test
    public void testGetAggregationReportId() throws IOException {
        System.out.println("getAggregationReportId");
        ReportManager instance = new ReportManager();
        instance.loadJson(new File(ReportManagerTest.class.getResource("/profiles/report.json").getFile()));

        String expResult = "00272336-7199-4ea4-bbe9-043aca02838c";
        String result = instance.getAggregationReportId();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of getMetricReportId method, of class ReportManager.
     */
    @Test
    public void testGetMetricReportId() throws IOException {
        System.out.println("getMetricReportId");
        ReportManager instance = new ReportManager();
        instance.loadJson(new File(ReportManagerTest.class.getResource("/profiles/report.json").getFile()));

        String expResult = "5850d7fe-75f5-4aa4-bacc-9a5c10280b59";
        String result = instance.getMetricReportId();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getOperationReportId method, of class ReportManager.
     */
    @Test
    public void testGetOperationReportId() throws IOException {
        System.out.println("getOperationReportId");
        ReportManager instance = new ReportManager();
        instance.loadJson(new File(ReportManagerTest.class.getResource("/profiles/report.json").getFile()));

        String expResult = "8ce59c4d-3761-4f25-a364-f019e394bf8b";
        String result = instance.getOperationReportId();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    private JsonElement loadJson(File jsonFile) throws IOException {

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(jsonFile));

            JsonParser json_parser = new JsonParser();
            JsonElement j_element = json_parser.parse(br);
//            JsonObject jRoot = j_element.getAsJsonObject();
//            JsonArray jData = jRoot.get("data").getAsJsonArray();
//            JsonElement jItem = jData.get(0);

            return j_element;
        } catch (FileNotFoundException ex) {
            //  LOG.error("Could not open file:" + jsonFile.getName());
            throw ex;

        } catch (JsonParseException ex) {
            //   LOG.error("File is not valid json:" + jsonFile.getName());
            throw ex;
        } finally {
            // Close quietly without exceptions the buffered reader
            IOUtils.closeQuietly(br);
        }

    }

}
