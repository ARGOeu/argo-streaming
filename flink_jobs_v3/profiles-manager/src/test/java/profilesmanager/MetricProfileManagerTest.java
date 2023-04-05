/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package profilesmanager;

import argo.avro.MetricProfile;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static junit.framework.Assert.assertNotNull;
import org.apache.commons.io.IOUtils;
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
public class MetricProfileManagerTest {

    public MetricProfileManagerTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        assertNotNull("Test file missing", MetricProfileManagerTest.class.getResource("/profiles/metricprofile.avro"));
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
     * Test of clear method, of class MetricProfileManager.
     */
    @Test
    public void testClear() throws IOException {
        System.out.println("clear");
        MetricProfileManager instance = new MetricProfileManager();

        instance.loadAvro(new File(MetricProfileManagerTest.class.getResource("/profiles/metricprofile.avro").getFile()));
        instance.clear();
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of indexInsertProfile method, of class MetricProfileManager.
     */
    @Test
    public void testIndexInsertProfile() throws IOException {
        System.out.println("indexInsertProfile");
        String profile = "ARGO_MON";
        MetricProfileManager instance = new MetricProfileManager();
       // instance.loadAvro(new File(MetricProfileManagerTest.class.getResource("/profiles/metricprofile.avro").getFile()));

        int expResult = 0;
        int result = instance.indexInsertProfile(profile);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of insert method, of class MetricProfileManager.
     */
    @Test
    public void testInsert() {
        System.out.println("insert");
        String profile = "Test_Profile";
        String service = "Test_Service";
        String metric = "Test_Mentric";
        HashMap<String, String> tags = new HashMap();
        tags.put("scope", "test");
        tags.put("comment", "to test");
        MetricProfileManager instance = new MetricProfileManager();
        instance.insert(profile, service, metric, tags);
        // TODO review the generated test code and remove the default call to fail.
        //   fail("The test case is a prototype.");
    }

    /**
     * Test of indexInsertService method, of class MetricProfileManager.
     */
    @Test
    public void testIndexInsertService() throws IOException {
        System.out.println("indexInsertService");
        MetricProfileManager instance = new MetricProfileManager();
     //   instance.loadAvro(new File(MetricProfileManagerTest.class.getResource("/profiles/metricprofile.avro").getFile()));

        String profile = "ARGO_MON";
        String service = "aai";
        int expResult = 0;
        int result = instance.indexInsertService(profile, service);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //     fail("The test case is a prototype.");
    }

    /**
     * Test of indexInsertMetric method, of class MetricProfileManager.
     */
    @Test
    public void testIndexInsertMetric() throws IOException {
        System.out.println("indexInsertMetric");
        MetricProfileManager instance = new MetricProfileManager();
        //      instance.loadAvro(new File(MetricProfileManagerTest.class.getResource("/profiles/metricprofile.avro").getFile()));

        String profile = "ARGO_MON";
        String service = "aai";
        String metric = "org.nagios.WebCheck";
        int expResult = 0;
        int result = instance.indexInsertMetric(profile, service, metric);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getProfileServices method, of class MetricProfileManager.
     */
    @Test
    public void testGetProfileServices() throws IOException {
        System.out.println("getProfileServices");
        MetricProfileManager instance = new MetricProfileManager();
        instance.loadAvro(new File(MetricProfileManagerTest.class.getResource("/profiles/metricprofile.avro").getFile()));

        String profile = "ARGO_MON";
        ArrayList<String> expResult = new ArrayList<>();
        expResult.add("nextcloud");
        expResult.add("aai");

        ArrayList<String> result = instance.getProfileServices(profile);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //    fail("The test case is a prototype.");
    }

    /**
     * Test of getProfiles method, of class MetricProfileManager.
     */
    @Test
    public void testGetProfiles() throws IOException {
        System.out.println("getProfiles");
        MetricProfileManager instance = new MetricProfileManager();
        instance.loadAvro(new File(MetricProfileManagerTest.class.getResource("/profiles/metricprofile.avro").getFile()));

        ArrayList<String> expResult = new ArrayList<>();
        expResult.add("ARGO_MON");
        ArrayList<String> result = instance.getProfiles();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of getProfileServiceMetrics method, of class MetricProfileManager.
     */
    @Test
    public void testGetProfileServiceMetrics() throws IOException {
        System.out.println("getProfileServiceMetrics");
        MetricProfileManager instance = new MetricProfileManager();
        instance.loadAvro(new File(MetricProfileManagerTest.class.getResource("/profiles/metricprofile.avro").getFile()));

        String profile = "ARGO_MON";
        String service = "aai";

        ArrayList<String> expResult = new ArrayList<>();
        expResult.add("org.nagios.WebCheck");
        expResult.add("argo.Keycloak-Check");

        ArrayList<String> result = instance.getProfileServiceMetrics(profile, service);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //   fail("The test case is a prototype.");
    }

    /**
     * Test of checkProfileServiceMetric method, of class MetricProfileManager.
     */
    @Test
    public void testCheckProfileServiceMetric() throws IOException {
        System.out.println("checkProfileServiceMetric");
        String profile = "ARGO_MON";
        String service = "aai";
        String metric = "org.nagios.WebCheck";
        MetricProfileManager instance = new MetricProfileManager();
        instance.loadAvro(new File(MetricProfileManagerTest.class.getResource("/profiles/metricprofile.avro").getFile()));

        boolean expResult = true;
        boolean result = instance.checkProfileServiceMetric(profile, service, metric);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of loadAvro method, of class MetricProfileManager.
     */
    @Test
    public void testLoadAvro() throws Exception {
        System.out.println("loadAvro");

        MetricProfileManager instance = new MetricProfileManager();
        instance.loadAvro(new File(MetricProfileManagerTest.class.getResource("/profiles/metricprofile.avro").getFile()));

        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of loadFromList method, of class MetricProfileManager.
     */
    @Test
    public void testLoadFromList() throws IOException {
        System.out.println("loadFromList");
        MetricProfileManager instance = new MetricProfileManager();
        JsonElement jsonElement=loadJson(new File(MetricProfileManagerTest.class.getResource("/profiles/metricprofile.json").getFile()));
      MetricProfile[] metricProfile=instance.readJson(jsonElement);
        List<MetricProfile> mps = Arrays.asList(metricProfile);
        instance.loadFromList(mps);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of loadMetricProfile method, of class MetricProfileManager.
     */
    @Test
    public void testLoadMetricProfile() throws Exception {
        System.out.println("loadMetricProfile");
           JsonElement jsonElement=loadJson(new File(MetricProfileManagerTest.class.getResource("/profiles/metricprofile.json").getFile()));
    //  MetricProfile[] metricProfile=instance.readJson(jsonElement);
    
        MetricProfileManager instance = new MetricProfileManager();
        instance.loadMetricProfile(jsonElement);
        // // TODO review the generated test code and remove the default call to fail.
       // fail("The test case is a prototype.");
    }

    /**
     * Test of readJson method, of class MetricProfileManager.
     */
//    @Test
//    public void testReadJson() {
//        System.out.println("readJson");
//        JsonElement jElement = null;
//        MetricProfileManager instance = new MetricProfileManager();
//        MetricProfile[] expResult = null;
//        MetricProfile[] result = instance.readJson(jElement);
//        assertArrayEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }

    /**
     * Test of containsMetric method, of class MetricProfileManager.
     */
    @Test
    public void testContainsMetric() throws IOException {
        System.out.println("containsMetric");
        MetricProfileManager instance = new MetricProfileManager();
        instance.loadAvro(new File(MetricProfileManagerTest.class.getResource("/profiles/metricprofile.avro").getFile()));

        String service = "aai";
        String metric = "org.nagios.WebCheck";

        boolean expResult = true;
        boolean result = instance.containsMetric(service, metric);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getList method, of class MetricProfileManager.
     */
//    @Test
//    public void testGetList() {
//        System.out.println("getList");
//        MetricProfileManager instance = new MetricProfileManager();
//        ArrayList expResult = null;
//        ArrayList result = instance.getList();
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
    /**
     * Test of setList method, of class MetricProfileManager.
     */
//    @Test
//    public void testSetList() {
//        System.out.println("setList");
//        ArrayList list = null;
//        MetricProfileManager instance = new MetricProfileManager();
//        instance.setList(list);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
    /**
     * Test of getIndex method, of class MetricProfileManager.
     */
//    @Test
//    public void testGetIndex() {
//        System.out.println("getIndex");
//        MetricProfileManager instance = new MetricProfileManager();
//        Map<String, HashMap<String, ArrayList<String>>> expResult = null;
//        Map<String, HashMap<String, ArrayList<String>>> result = instance.getIndex();
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of setIndex method, of class MetricProfileManager.
//     */
//    @Test
//    public void testSetIndex() {
//        System.out.println("setIndex");
//        Map<String, HashMap<String, ArrayList<String>>> index = null;
//        MetricProfileManager instance = new MetricProfileManager();
//        instance.setIndex(index);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
    
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
