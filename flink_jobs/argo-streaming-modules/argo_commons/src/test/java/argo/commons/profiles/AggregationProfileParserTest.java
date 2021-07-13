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
public class AggregationProfileParserTest {

    public AggregationProfileParserTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        assertNotNull("Test file missing", AggregationProfileParserTest.class.getResource("/profiles/aggregation.json"));
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
     * Test of retrieveServiceFunctions method, of class
     * AggregationProfileParser.
     */
    @Test
    public void testRetrieveServiceFunctions() throws IOException, ParseException {
        System.out.println("retrieveServiceFunctions");
        String service = "ARC-CE";
        JSONObject aggrJSONObject = readJsonFromFile(AggregationProfileParserTest.class.getResource("/profiles/aggregation.json").getFile());
        AggregationProfileParser instance = new AggregationProfileParser(aggrJSONObject);

        ArrayList<String> expResult = new ArrayList<>();
        expResult.add("compute");
        ArrayList<String> result = instance.retrieveServiceFunctions(service);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //    fail("The test case is a prototype.");
    }

    /**
     * Test of getServiceOperation method, of class AggregationProfileParser.
     */
    @Test
    public void testGetServiceOperation() throws IOException, ParseException {
        System.out.println("getServiceOperation");

        String service = "ARC-CE";
        JSONObject aggrJSONObject = readJsonFromFile(AggregationProfileParserTest.class.getResource("/profiles/aggregation.json").getFile());
        AggregationProfileParser instance = new AggregationProfileParser(aggrJSONObject);

        String expResult = "OR";
        String result = instance.getServiceOperation(service);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getFunctionOperation method, of class AggregationProfileParser.
     */
    @Test
    public void testGetFunctionOperation() throws IOException, ParseException {
        System.out.println("getFunctionOperation");
        String function = "cloud";
       JSONObject aggrJSONObject = readJsonFromFile(AggregationProfileParserTest.class.getResource("/profiles/aggregation.json").getFile());
        AggregationProfileParser instance = new AggregationProfileParser(aggrJSONObject);


        String expResult = "OR";
        String result = instance.getFunctionOperation(function);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getId method, of class AggregationProfileParser.
     */
    @Test
    public void testGetId() throws IOException, ParseException {
        System.out.println("getId");
       JSONObject aggrJSONObject = readJsonFromFile(AggregationProfileParserTest.class.getResource("/profiles/aggregation.json").getFile());
        AggregationProfileParser instance = new AggregationProfileParser(aggrJSONObject);

        String expResult = "00272336-7199-4ea4-bbe9-043aca02838c";
        String result = instance.getId();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getDate method, of class AggregationProfileParser.
     */
    @Test
    public void testGetDate() throws IOException, ParseException {
        System.out.println("getDate");
       JSONObject aggrJSONObject = readJsonFromFile(AggregationProfileParserTest.class.getResource("/profiles/aggregation.json").getFile());
        AggregationProfileParser instance = new AggregationProfileParser(aggrJSONObject);

        String expResult = "2021-01-01";
        String result = instance.getDate();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getName method, of class AggregationProfileParser.
     */
    @Test
    public void testGetName() throws IOException, ParseException {
        System.out.println("getName");
       JSONObject aggrJSONObject = readJsonFromFile(AggregationProfileParserTest.class.getResource("/profiles/aggregation.json").getFile());
        AggregationProfileParser instance = new AggregationProfileParser(aggrJSONObject);

        String expResult = "sla_test";
        String result = instance.getName();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getNamespace method, of class AggregationProfileParser.
     */
    @Test
    public void testGetNamespace() throws IOException, ParseException {
        System.out.println("getNamespace");
       JSONObject aggrJSONObject = readJsonFromFile(AggregationProfileParserTest.class.getResource("/profiles/aggregation.json").getFile());
        AggregationProfileParser instance = new AggregationProfileParser(aggrJSONObject);

        String expResult = "";
        String result = instance.getNamespace();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getEndpointGroup method, of class AggregationProfileParser.
     */
    @Test
    public void testGetEndpointGroup() throws IOException, ParseException {
        System.out.println("getEndpointGroup");
       JSONObject aggrJSONObject = readJsonFromFile(AggregationProfileParserTest.class.getResource("/profiles/aggregation.json").getFile());
        AggregationProfileParser instance = new AggregationProfileParser(aggrJSONObject);

        String expResult = "servicegroups";
        String result = instance.getEndpointGroup();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //   fail("The test case is a prototype.");
    }

    /**
     * Test of getMetricOp method, of class AggregationProfileParser.
     */
    @Test
    public void testGetMetricOp() throws IOException, ParseException {
        System.out.println("getMetricOp");
       JSONObject aggrJSONObject = readJsonFromFile(AggregationProfileParserTest.class.getResource("/profiles/aggregation.json").getFile());
        AggregationProfileParser instance = new AggregationProfileParser(aggrJSONObject);

        String expResult = "AND";
        String result = instance.getMetricOp();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getProfileOp method, of class AggregationProfileParser.
     */
    @Test
    public void testGetProfileOp() throws IOException, ParseException {
        System.out.println("getProfileOp");
       JSONObject aggrJSONObject = readJsonFromFile(AggregationProfileParserTest.class.getResource("/profiles/aggregation.json").getFile());
        AggregationProfileParser instance = new AggregationProfileParser(aggrJSONObject);

        String expResult = "AND";
        String result = instance.getProfileOp();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of getMetricProfile method, of class AggregationProfileParser.
     */
    @Test
    public void testGetMetricProfile() throws IOException, ParseException {
        System.out.println("getMetricProfile");
       JSONObject aggrJSONObject = readJsonFromFile(AggregationProfileParserTest.class.getResource("/profiles/aggregation.json").getFile());
        AggregationProfileParser instance = new AggregationProfileParser(aggrJSONObject);

        String[] expResult = {"5850d7fe-75f5-4aa4-bacc-9a5c10280b59", "ARGO_MON_CRITICAL"};

        String[] result = instance.getMetricProfile();
        assertArrayEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getGroups method, of class AggregationProfileParser.
     */
    @Test
    public void testGetGroups() throws IOException, ParseException {
        System.out.println("getGroups");
        JSONObject aggrJSONObject = readJsonFromFile(AggregationProfileParserTest.class.getResource("/profiles/aggregation.json").getFile());
        AggregationProfileParser instance = new AggregationProfileParser(aggrJSONObject);

        HashMap<String, String> map1 = new HashMap();
        map1.put("ARC-CE", "OR");
        map1.put("GRAM5", "OR");
        map1.put("QCG.Computing", "OR");
        map1.put("org.opensciencegrid.htcondorce", "OR");

        AggregationProfileParser.GroupOps groupOp = instance.new GroupOps("compute", "OR", map1);

        map1 = new HashMap();
        map1.put("SRM", "OR");
        map1.put("SRM", "OR");

        AggregationProfileParser.GroupOps groupOp1 = instance.new GroupOps("storage", "OR", map1);

        map1 = new HashMap();
        map1.put("Site-BDII", "OR");

        AggregationProfileParser.GroupOps groupOp2 = instance.new GroupOps("information", "OR", map1);
        map1 = new HashMap();
        map1.put("eu.egi.cloud.vm-management.occi", "OR");
        map1.put("org.openstack.nova", "OR");

        AggregationProfileParser.GroupOps groupOp3 = instance.new GroupOps("cloud", "OR", map1);

        ArrayList<AggregationProfileParser.GroupOps> expResult = new ArrayList<>();
        expResult.add(groupOp);
        expResult.add(groupOp1);
        expResult.add(groupOp2);
        expResult.add(groupOp3);

        ArrayList<AggregationProfileParser.GroupOps> result = instance.getGroups();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getServiceOperations method, of class AggregationProfileParser.
     */
    @Test
    public void testGetServiceOperations() throws IOException, ParseException {
        System.out.println("getServiceOperations");
       JSONObject aggrJSONObject = readJsonFromFile(AggregationProfileParserTest.class.getResource("/profiles/aggregation.json").getFile());
        AggregationProfileParser instance = new AggregationProfileParser(aggrJSONObject);

        HashMap<String, String> expResult = new HashMap<>();

        expResult.put("ARC-CE", "OR");
        expResult.put("GRAM5", "OR");
        expResult.put("QCG.Computing", "OR");
        expResult.put("org.opensciencegrid.htcondorce", "OR");
        expResult.put("SRM", "OR");
        expResult.put("Site-BDII", "OR");
        expResult.put("eu.egi.cloud.vm-management.occi", "OR");
        expResult.put("org.openstack.nova", "OR");
        HashMap<String, String> result = instance.getServiceOperations();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of setServiceOperations method, of class AggregationProfileParser.
     */
    @Test
    public void testSetServiceOperations() throws IOException, ParseException {
        System.out.println("setServiceOperations");

      JSONObject aggrJSONObject = readJsonFromFile(AggregationProfileParserTest.class.getResource("/profiles/aggregation.json").getFile());
        AggregationProfileParser instance = new AggregationProfileParser(aggrJSONObject);

        HashMap<String, String> serviceOperations = new HashMap<>();

        serviceOperations.put("ARC-CE", "OR");
        serviceOperations.put("GRAM5", "OR");
        serviceOperations.put("QCG.Computing", "OR");
        serviceOperations.put("org.opensciencegrid.htcondorce", "OR");
        serviceOperations.put("SRM", "OR");
        serviceOperations.put("Site-BDII", "OR");
        serviceOperations.put("eu.egi.cloud.vm-management.occi", "OR");
        serviceOperations.put("org.openstack.nova", "OR");
        instance.setServiceOperations(serviceOperations);
        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
    }

    /**
     * Test of getFunctionOperations method, of class AggregationProfileParser.
     */
    @Test
    public void testGetFunctionOperations() throws IOException, ParseException {
        System.out.println("getFunctionOperations");

      JSONObject aggrJSONObject = readJsonFromFile(AggregationProfileParserTest.class.getResource("/profiles/aggregation.json").getFile());
        AggregationProfileParser instance = new AggregationProfileParser(aggrJSONObject);

        HashMap<String, String> expResult = new HashMap<>();
        expResult.put("compute", "OR");
        expResult.put("storage", "OR");
        expResult.put("information", "OR");
        expResult.put("cloud", "OR");
        HashMap<String, String> result = instance.getFunctionOperations();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //      fail("The test case is a prototype.");
    }

    /**
     * Test of setFunctionOperations method, of class AggregationProfileParser.
     */
    @Test
    public void testSetFunctionOperations() throws IOException, ParseException {
        System.out.println("setFunctionOperations");

       JSONObject aggrJSONObject = readJsonFromFile(AggregationProfileParserTest.class.getResource("/profiles/aggregation.json").getFile());
        AggregationProfileParser instance = new AggregationProfileParser(aggrJSONObject);

        HashMap<String, String> functionOperations = new HashMap<>();
        functionOperations.put("compute", "OR");
        functionOperations.put("storage", "OR");
        functionOperations.put("information", "OR");
        functionOperations.put("cloud", "OR");

        instance.setFunctionOperations(functionOperations);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of getServiceFunctions method, of class AggregationProfileParser.
     */
    @Test
    public void testGetServiceFunctions() throws IOException, ParseException {
        System.out.println("getServiceFunctions");
        JSONObject aggrJSONObject = readJsonFromFile(AggregationProfileParserTest.class.getResource("/profiles/aggregation.json").getFile());
        AggregationProfileParser instance = new AggregationProfileParser(aggrJSONObject);

        HashMap<String, ArrayList<String>> expResult = new HashMap();

        expResult.put("ARC-CE", new ArrayList<>(Arrays.asList("compute")));
        expResult.put("GRAM5", new ArrayList<>(Arrays.asList("compute")));
        expResult.put("QCG.Computing", new ArrayList<>(Arrays.asList("compute")));
        expResult.put("org.opensciencegrid.htcondorce", new ArrayList<>(Arrays.asList("compute")));
        expResult.put("SRM", new ArrayList<>(Arrays.asList("storage")));
        expResult.put("Site-BDII", new ArrayList<>(Arrays.asList("information")));
        expResult.put("eu.egi.cloud.vm-management.occi", new ArrayList<>(Arrays.asList("cloud")));
        expResult.put("org.openstack.nova", new ArrayList<>(Arrays.asList("cloud")));

        HashMap<String, ArrayList<String>> result = instance.getServiceFunctions();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of setServiceFunctions method, of class AggregationProfileParser.
     */
    @Test
    public void testSetServiceFunctions() throws IOException, ParseException {
        System.out.println("setServiceFunctions");
        // HashMap<String, ArrayList<String>> serviceFunctions = null;

       JSONObject aggrJSONObject = readJsonFromFile(AggregationProfileParserTest.class.getResource("/profiles/aggregation.json").getFile());
        AggregationProfileParser instance = new AggregationProfileParser(aggrJSONObject);

        HashMap<String, ArrayList<String>> serviceFunctions = new HashMap();

        serviceFunctions.put("ARC-CE", new ArrayList<>(Arrays.asList("compute")));
        serviceFunctions.put("GRAM5", new ArrayList<>(Arrays.asList("compute")));
        serviceFunctions.put("QCG.Computing", new ArrayList<>(Arrays.asList("compute")));
        serviceFunctions.put("org.opensciencegrid.htcondorce", new ArrayList<>(Arrays.asList("compute")));
        serviceFunctions.put("SRM", new ArrayList<>(Arrays.asList("storage")));
        serviceFunctions.put("Site-BDII", new ArrayList<>(Arrays.asList("information")));
        serviceFunctions.put("eu.egi.cloud.vm-management.occi", new ArrayList<>(Arrays.asList("cloud")));
        serviceFunctions.put("org.openstack.nova", new ArrayList<>(Arrays.asList("cloud")));

        instance.setServiceFunctions(serviceFunctions);
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
