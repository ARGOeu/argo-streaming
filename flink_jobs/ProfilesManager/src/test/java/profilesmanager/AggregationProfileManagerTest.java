/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package profilesmanager;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import static junit.framework.Assert.assertNotNull;
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
public class AggregationProfileManagerTest {

    public AggregationProfileManagerTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        assertNotNull("Test file missing", AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json"));
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
     * Test of clearProfiles method, of class AggregationProfileManager.
     */
    @Test
    public void testClearProfiles() throws IOException {
        System.out.println("clearProfiles");
        AggregationProfileManager instance = new AggregationProfileManager();
        File file = new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile());
        instance.loadJson(file);

        instance.clearProfiles();
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of getTotalOp method, of class AggregationProfileManager.
     */
    @Test
    public void testGetTotalOp() throws IOException {
        System.out.println("getTotalOp");
        AggregationProfileManager instance = new AggregationProfileManager();
        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));
        AggregationProfileManager.AvProfileItem avProfileItem = instance.getAvProfileItem();

        String expResult = avProfileItem.getOp();
        String result = instance.getTotalOp(avProfileItem.getName());
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of getMetricOp method, of class AggregationProfileManager.
     */
    @Test
    public void testGetMetricOp() throws IOException {
        System.out.println("getMetricOp");
        AggregationProfileManager instance = new AggregationProfileManager();
        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));
        AggregationProfileManager.AvProfileItem avProfileItem = instance.getAvProfileItem();

        String expResult = avProfileItem.getMetricOp();
        String result = instance.getTotalOp(avProfileItem.getName());
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of getProfileGroups method, of class AggregationProfileManager.
     */
    @Test
    public void testGetProfileGroups() throws IOException {
        System.out.println("getProfileGroups");
        AggregationProfileManager instance = new AggregationProfileManager();
        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));
        AggregationProfileManager.AvProfileItem avProfileItem = instance.getAvProfileItem();

        ArrayList<String> expResult = new ArrayList<>();
        expResult.add("compute");
        expResult.add("cloud");
        expResult.add("information");
        expResult.add("storage");

        ArrayList<String> result = instance.getProfileGroups(avProfileItem.getName());
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getProfileGroupOp method, of class AggregationProfileManager.
     */
    @Test
    public void testGetProfileGroupOp() throws IOException {
        System.out.println("getProfileGroupOp");
        AggregationProfileManager instance = new AggregationProfileManager();
        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));
        AggregationProfileManager.AvProfileItem avProfileItem = instance.getAvProfileItem();
        String avProfile = avProfileItem.getName();
        String groupName = "compute";

        String expResult = "OR";
        String result = instance.getProfileGroupOp(avProfile, groupName);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of getProfileGroupServices method, of class
     * AggregationProfileManager.
     */
    @Test
    public void testGetProfileGroupServices() throws IOException {
        System.out.println("getProfileGroupServices");

        AggregationProfileManager instance = new AggregationProfileManager();
        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));
        AggregationProfileManager.AvProfileItem avProfileItem = instance.getAvProfileItem();
        String avProfile = avProfileItem.getName();
        String groupName = "compute";
        ArrayList<String> expResult = new ArrayList<>();
        expResult.add("ARC-CE");
        expResult.add("GRAM5");
        expResult.add("QCG.Computing");
        expResult.add("org.opensciencegrid.htcondorce");

        ArrayList<String> result = instance.getProfileGroupServices(avProfile, groupName);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getProfileGroupServiceOp method, of class
     * AggregationProfileManager.
     */
    @Test
    public void testGetProfileGroupServiceOp() throws IOException {
        System.out.println("getProfileGroupServiceOp");
        AggregationProfileManager instance = new AggregationProfileManager();
        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));
        AggregationProfileManager.AvProfileItem avProfileItem = instance.getAvProfileItem();
        String avProfile = avProfileItem.getName();
        String groupName = "compute";
        String service = "ARC-CE";

        String expResult = "OR";
        String result = instance.getProfileGroupServiceOp(avProfile, groupName, service);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of getAvProfiles method, of class AggregationProfileManager.
     */
    @Test
    public void testGetAvProfiles() throws IOException {
        System.out.println("getAvProfiles");
        AggregationProfileManager instance = new AggregationProfileManager();
        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));

        ArrayList<String> expResult = new ArrayList<>();
        expResult.add("sla_test");
        ArrayList<String> result = instance.getAvProfiles();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of getProfileNamespace method, of class AggregationProfileManager.
     */
    @Test
    public void testGetProfileNamespace() throws IOException {
        System.out.println("getProfileNamespace");
        String avProfile = "sla_test";
        AggregationProfileManager instance = new AggregationProfileManager();
        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));

        String expResult = "";
        String result = instance.getProfileNamespace(avProfile);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getProfileMetricProfile method, of class
     * AggregationProfileManager.
     */
    @Test
    public void testGetProfileMetricProfile() throws IOException {
        System.out.println("getProfileMetricProfile");
        String avProfile = "sla_test";
        AggregationProfileManager instance = new AggregationProfileManager();
        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));

        String expResult = "ARGO_MON_CRITICAL";
        String result = instance.getProfileMetricProfile(avProfile);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getProfileGroupType method, of class AggregationProfileManager.
     */
    @Test
    public void testGetProfileGroupType() throws IOException {
        System.out.println("getProfileGroupType");
        String avProfile = "sla_test";
        AggregationProfileManager instance = new AggregationProfileManager();
        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));

        String expResult = "servicegroups";
        String result = instance.getProfileGroupType(avProfile);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getGroupByService method, of class AggregationProfileManager.
     */
    @Test
    public void testGetGroupByService() throws IOException {
        System.out.println("getGroupByService");
        String avProfile = "sla_test";
        String service = "ARC-CE";
        AggregationProfileManager instance = new AggregationProfileManager();
        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));

        String expResult = "compute";
        String result = instance.getGroupByService(avProfile, service);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of checkService method, of class AggregationProfileManager.
     */
    @Test
    public void testCheckService() throws IOException {
        System.out.println("checkService");
        String avProfile = "sla_test";
        String service = "ARC-CE";
        AggregationProfileManager instance = new AggregationProfileManager();
        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));

        boolean expResult = true;
        boolean result = instance.checkService(avProfile, service);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of loadJson method, of class AggregationProfileManager.
     */
    @Test
    public void testLoadJson() throws Exception {
        System.out.println("loadJson");
        AggregationProfileManager instance = new AggregationProfileManager();
        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

//    /**
//     * Test of loadJsonString method, of class AggregationProfileManager.
//     */
//    @Test
//    public void testLoadJsonString() throws Exception {
//        System.out.println("loadJsonString");
//        List<String> apsJson = null;
//              AggregationProfileManager instance = new AggregationProfileManager();
//        JsonElement jsonElement=instance.loadJson(new File(MetricProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));
//      MetricProfile[] metricProfile=instance.readJson(jsonElement);
//        List<MetricProfile> mps = Arrays.asList(metricProfile);
// 
//        instance.loadJsonString(apsJson);
//        //// TODO review the generated test code and remove the default call to fail.
//        // fail("The test case is a prototype.");
//    }
//    /**
//     * Test of readJson method, of class AggregationProfileManager.
//     */
//    @Test
//    public void testReadJson() {
//        System.out.println("readJson");
//        JsonElement j_element = null;
//        AggregationProfileManager instance = new AggregationProfileManager();
//        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));
//       
//        instance.readJson(j_element);
//        // TODO review the generated test code and remove the default call to fail.
//        // fail("The test case is a prototype.");
//    }
    /**
     * Test of getEndpointGroup method, of class AggregationProfileManager.
     */
    @Test
    public void testGetEndpointGroup() throws IOException {
        System.out.println("getEndpointGroup");
        AggregationProfileManager instance = new AggregationProfileManager();
        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));

        String expResult = "servicegroups";
        String result = instance.getProfileGroupType("sla_test");
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of retrieveFunctionsByService method, of class
     * AggregationProfileManager.
     */
//    @Test
//    public void testRetrieveFunctionsByService() throws IOException {
//        System.out.println("retrieveFunctionsByService");
//        String service = "ARC-CE";
//        AggregationProfileManager instance = new AggregationProfileManager();
//        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));
//
//        ArrayList<String> expResult = new ArrayList<>();
//        expResult.add("compute");
//        ArrayList<String> result = instance.r(service);
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        // fail("The test case is a prototype.");
//    }
    /**
     * Test of retrieveServiceOperations method, of class
     * AggregationProfileManager.
     */
    @Test
    public void testRetrieveServiceOperations() throws IOException {
        System.out.println("retrieveServiceOperations");
        AggregationProfileManager instance = new AggregationProfileManager();
        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));

        HashMap<String, String> expResult = new HashMap<String, String>();
        expResult.put("ARC-CE", "OR");
        expResult.put("GRAM5", "OR");
        expResult.put("QCG.Computing", "OR");
        expResult.put("org.opensciencegrid.htcondorce", "OR");
        expResult.put("SRM", "OR");
        expResult.put("Site-BDII", "OR");
        expResult.put("org.openstack.nova", "OR");

        HashMap<String, String> result = instance.retrieveServiceOperations();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of retrieveServiceFunctions method, of class
     * AggregationProfileManager.
     */
    @Test
    public void testRetrieveServiceFunctions() throws IOException {
        System.out.println("retrieveServiceFunctions");
        AggregationProfileManager instance = new AggregationProfileManager();
        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));

        HashMap<String, String> expResult = new HashMap<>();
        expResult.put("ARC-CE", "compute");
        expResult.put("GRAM5", "compute");
        expResult.put("QCG.Computing", "compute");
        expResult.put("org.opensciencegrid.htcondorce", "compute");
        expResult.put("SRM", "storage");
        expResult.put("Site-BDII", "information");
        expResult.put("org.openstack.nova", "cloud");

        HashMap<String, String> result = instance.retrieveServiceFunctions();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of checkServiceExistance method, of class AggregationProfileManager.
     */
    @Test
    public void testCheckServiceExistance() throws IOException {
        System.out.println("checkServiceExistance");
        String service = "ARC-CE";
        AggregationProfileManager instance = new AggregationProfileManager();
        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));

        boolean expResult = true;
        boolean result = instance.checkServiceExistance(service);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getAvProfileItem method, of class AggregationProfileManager.
     */
    @Test
    public void testGetAvProfileItem() throws IOException {
        System.out.println("getAvProfileItem");
        AggregationProfileManager instance = new AggregationProfileManager();
        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));
        AggregationProfileManager.AvProfileItem expResult = instance.getAvProfileItem();
        AggregationProfileManager.AvProfileItem result = instance.getAvProfileItem();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getMetricOpByProfile method, of class AggregationProfileManager.
     */
    @Test
    public void testGetMetricOpByProfile() throws IOException {
        System.out.println("getMetricOpByProfile");
        AggregationProfileManager instance = new AggregationProfileManager();
        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));

        String expResult = "AND";
        String result = instance.getMetricOpByProfile();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of retrieveGroupFunctions method, of class
     * AggregationProfileManager.
     */
    @Test
    public void testRetrieveGroupOperations() throws IOException {
        System.out.println("retrieveGroupFunctions");
        AggregationProfileManager instance = new AggregationProfileManager();
        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));

        HashMap<String, String> expResult = new HashMap<>();
        expResult.put("compute", "OR");
        expResult.put("cloud", "OR");
        expResult.put("information", "OR");
        expResult.put("storage", "OR");

        HashMap<String, String> result = instance.retrieveGroupOperations();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of retrieveGroupOperation method, of class
     * AggregationProfileManager.
     */
    @Test
    public void testRetrieveGroupOperation() throws IOException {
        System.out.println("retrieveGroupOperation");
        String group = "compute";
        AggregationProfileManager instance = new AggregationProfileManager();
        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));

        String expResult = "OR";
        String result = instance.retrieveGroupOperation(group);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of retrieveProfileOperation method, of class
     * AggregationProfileManager.
     */
    @Test
    public void testRetrieveProfileOperation() throws IOException {
        System.out.println("retrieveProfileOperation");
        AggregationProfileManager instance = new AggregationProfileManager();
        instance.loadJson(new File(AggregationProfileManagerTest.class.getResource("/profiles/aggregations.json").getFile()));

        String expResult = "AND";
        String result = instance.retrieveProfileOperation();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

//    /**
//     * Test of loadJsonString method, of class AggregationProfileManager.
//     */
//    @Test
//    public void testLoadJsonString() throws Exception {
//        System.out.println("loadJsonString");
//        List<String> apsJson = null;
//        AggregationProfileManager instance = new AggregationProfileManager();
//        instance.loadJsonString(apsJson);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of readJson method, of class AggregationProfileManager.
//     */
//    @Test
//    public void testReadJson() {
//        System.out.println("readJson");
//        JsonElement j_element = null;
//        AggregationProfileManager instance = new AggregationProfileManager();
//        instance.readJson(j_element);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of retrieveFunctionsByService method, of class AggregationProfileManager.
//     */
//    @Test
//    public void testRetrieveFunctionsByService() {
//        System.out.println("retrieveFunctionsByService");
//        String service = "";
//        AggregationProfileManager instance = new AggregationProfileManager();
//        String expResult = "";
//        String result = instance.retrieveFunctionsByService(service);
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
}
