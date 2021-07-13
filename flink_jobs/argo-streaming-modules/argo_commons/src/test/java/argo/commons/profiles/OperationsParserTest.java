package argo.commons.profiles;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import argo.commons.profiles.OperationsParser;
import argo.commons.timelines.TimelineMergerTest;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
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
public class OperationsParserTest {

    public OperationsParserTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        assertNotNull("Test file missing", OperationsParserTest.class.getResource("/profiles/operations.json"));
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
     * Test of loadOperationProfile method, of class OperationsParser.
     */
    @Test
    public void testLoadOperationProfile() throws Exception {
        System.out.println("loadOperationProfile");

        JSONObject oppJSONObject = readJsonFromFile(OperationsParserTest.class.getResource("/profiles/operations.json").getFile());
        OperationsParser instance = new OperationsParser(oppJSONObject);

        HashMap<String, HashMap<String, String>> expResult = new HashMap<>();
        expResult.put("AND", createAndOp());
        expResult.put("OR", createOrOp());
        HashMap<String, HashMap<String, String>> result = instance.getOpTruthTable();
        assertEquals(expResult.toString(), result.toString());
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getId method, of class OperationsParser.
     */
    @Test
    public void testGetId() throws IOException, ParseException {
        System.out.println("getId");

        String operationsId = "8ce59c4d-3761-4f25-a364-f019e394bf8b";

        JSONObject oppJSONObject = readJsonFromFile(OperationsParserTest.class.getResource("/profiles/operations.json").getFile());
        OperationsParser instance = new OperationsParser(oppJSONObject);

        String expResult = operationsId;
        String result = instance.getId();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getName method, of class OperationsParser.
     */
    @Test
    public void testGetName() throws IOException, ParseException {
        System.out.println("getName");

        JSONObject oppJSONObject = readJsonFromFile(OperationsParserTest.class.getResource("/profiles/operations.json").getFile());
        OperationsParser instance = new OperationsParser(oppJSONObject);

        String expResult = "egi_ops";
        String result = instance.getName();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
    }

    /**
     * Test of getStates method, of class OperationsParser.
     */
    @Test
    public void testGetStates() throws IOException, ParseException {
        System.out.println("getStates");

        JSONObject oppJSONObject = readJsonFromFile(OperationsParserTest.class.getResource("/profiles/operations.json").getFile());
        OperationsParser instance = new OperationsParser(oppJSONObject);

        ArrayList<String> expResult = new ArrayList<>();

        expResult.add("OK");
        expResult.add("WARNING");
        expResult.add("UNKNOWN");
        expResult.add("MISSING");
        expResult.add("CRITICAL");
        expResult.add("DOWNTIME");

        ArrayList<String> result = instance.getStates();
        assertEquals(expResult.toString(), result.toString());
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getDefaults method, of class OperationsParser.
     */
    @Test
    public void testGetDefaults() throws IOException, ParseException {
        System.out.println("getDefaults");

       
        JSONObject oppJSONObject = readJsonFromFile(OperationsParserTest.class.getResource("/profiles/operations.json").getFile());
        OperationsParser instance = new OperationsParser(oppJSONObject);

        OperationsParser.DefaultStatus expResult = instance.new DefaultStatus("DOWNTIME", "MISSING", "UNKNOWN");
        OperationsParser.DefaultStatus result = instance.getDefaults();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //   fail("The test case is a prototype.");
    }

    /**
     * Test of getOpTruthTable method, of class OperationsParser.
     */
    @Test
    public void testGetOpTruthTable() throws IOException, ParseException {
        System.out.println("getOpTruthTable");

       
        JSONObject oppJSONObject = readJsonFromFile(OperationsParserTest.class.getResource("/profiles/operations.json").getFile());
        OperationsParser instance = new OperationsParser(oppJSONObject);

        HashMap<String, HashMap<String, String>> expResult = new HashMap<>();
        expResult.put("AND", createAndOp());
        expResult.put("OR", createOrOp());
        HashMap<String, HashMap<String, String>> result = instance.getOpTruthTable();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of setId method, of class OperationsParser.
     */
    @Test
    public void testSetId() throws IOException, ParseException {
        System.out.println("setId");
         String operationsId = "8ce59c4d-3761-4f25-a364-f019e394bf8b";

       
        JSONObject oppJSONObject = readJsonFromFile(OperationsParserTest.class.getResource("/profiles/operations.json").getFile());
        OperationsParser instance = new OperationsParser(oppJSONObject);

        instance.setId(operationsId);
        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
    }

    /**
     * Test of setName method, of class OperationsParser.
     */
    @Test
    public void testSetName() throws IOException, ParseException {
        System.out.println("setName");
        String name = "egi_ops";
      
        JSONObject oppJSONObject = readJsonFromFile(OperationsParserTest.class.getResource("/profiles/operations.json").getFile());
        OperationsParser instance = new OperationsParser(oppJSONObject);

        instance.setName(name);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of setStates method, of class OperationsParser.
     */
    @Test
    public void testSetStates() throws IOException, ParseException {
        System.out.println("setStates");

        JSONObject oppJSONObject = readJsonFromFile(OperationsParserTest.class.getResource("/profiles/operations.json").getFile());
        OperationsParser instance = new OperationsParser(oppJSONObject);

        ArrayList<String> states = new ArrayList<>();
      
        states.add("OK");
        states.add("WARNING");
        states.add("UNKNOWN");
        states.add("MISSING");
        states.add("CRITICAL");
        states.add("DOWNTIME");

        instance.setStates(states);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of setDefaults method, of class OperationsParser.
     */
    @Test
    public void testSetDefaults() throws IOException, ParseException {
        System.out.println("setDefaults");

       
        JSONObject oppJSONObject = readJsonFromFile(OperationsParserTest.class.getResource("/profiles/operations.json").getFile());
        OperationsParser instance = new OperationsParser(oppJSONObject);

        OperationsParser.DefaultStatus defaults = instance.new DefaultStatus("DOWNTIME", "MISSING", "UNKNOWN");

        instance.setDefaults(defaults);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of setOpTruthTable method, of class OperationsParser.
     */
    @Test
    public void testSetOpTruthTable() throws IOException, ParseException {
        System.out.println("setOpTruthTable");
        HashMap<String, HashMap<String, String>> opTruthTable = new HashMap<>();
        opTruthTable.put("AND", createAndOp());
        opTruthTable.put("OR", createOrOp());

        JSONObject oppJSONObject = readJsonFromFile(OperationsParserTest.class.getResource("/profiles/operations.json").getFile());
        OperationsParser instance = new OperationsParser(oppJSONObject);

        instance.setOpTruthTable(opTruthTable);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getStatusFromTruthTable method, of class OperationsParser.
     */
    @Test
    public void testGetStatusFromTruthTable() throws IOException, ParseException {
        System.out.println("getStatusFromTruthTable");
        String operation = "AND";
        String astatus = "OK";
        String bstatus = "OK";

       
        JSONObject oppJSONObject = readJsonFromFile(OperationsParserTest.class.getResource("/profiles/operations.json").getFile());
        OperationsParser instance = new OperationsParser(oppJSONObject);


        String expResult = "OK";
        String result = instance.getStatusFromTruthTable(operation, astatus, bstatus);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //   fail("The test case is a prototype.");
    }

    private HashMap<String, String> createAndOp() {

        HashMap<String, String> opMap = new HashMap();

        opMap.put("OK-OK", "OK");

        opMap.put("OK-WARNING", "WARNING");

        opMap.put("OK-UNKNOWN", "UNKNOWN");

        opMap.put("OK-MISSING", "MISSING");

        opMap.put("OK-CRITICAL", "CRITICAL");

        opMap.put("OK-DOWNTIME", "DOWNTIME");

        opMap.put("WARNING-WARNING", "WARNING");

        opMap.put("WARNING-UNKNOWN", "UNKNOWN");

        opMap.put("WARNING-MISSING", "MISSING");

        opMap.put("WARNING-CRITICAL", "CRITICAL");

        opMap.put("WARNING-DOWNTIME", "DOWNTIME");

        opMap.put("UNKNOWN-UNKNOWN", "UNKNOWN");

        opMap.put("UNKNOWN-MISSING", "MISSING");

        opMap.put("UNKNOWN-CRITICAL", "CRITICAL");

        opMap.put("UNKNOWN-DOWNTIME", "DOWNTIME");

        opMap.put("MISSING-MISSING", "MISSING");

        opMap.put("MISSING-CRITICAL", "CRITICAL");

        opMap.put("MISSING-DOWNTIME", "DOWNTIME");

        opMap.put("CRITICAL-CRITICAL", "CRITICAL");

        opMap.put("CRITICAL-DOWNTIME", "CRITICAL");

        opMap.put("DOWNTIME-DOWNTIME", "DOWNTIME");
        return opMap;
    }

    private HashMap<String, String> createOrOp() {

        HashMap<String, String> opMap = new HashMap();

        opMap.put("OK-OK", "OK");

        opMap.put("OK-WARNING", "OK");

        opMap.put("OK-UNKNOWN", "OK");

        opMap.put("OK-MISSING", "OK");

        opMap.put("OK-CRITICAL", "OK");

        opMap.put("OK-DOWNTIME", "OK");

        opMap.put("WARNING-WARNING", "WARNING");

        opMap.put("WARNING-UNKNOWN", "WARNING");

        opMap.put("WARNING-MISSING", "WARNING");

        opMap.put("WARNING-CRITICAL", "WARNING");

        opMap.put("WARNING-DOWNTIME", "WARNING");

        opMap.put("UNKNOWN-UNKNOWN", "UNKNOWN");

        opMap.put("UNKNOWN-MISSING", "UNKNOWN");

        opMap.put("UNKNOWN-CRITICAL", "CRITICAL");

        opMap.put("UNKNOWN-DOWNTIME", "UNKNOWN");

        opMap.put("MISSING-MISSING", "MISSING");

        opMap.put("MISSING-CRITICAL", "CRITICAL");

        opMap.put("MISSING-DOWNTIME", "DOWNTIME");

        opMap.put("CRITICAL-CRITICAL", "CRITICAL");

        opMap.put("CRITICAL-DOWNTIME", "CRITICAL");

        opMap.put("DOWNTIME-DOWNTIME", "DOWNTIME");

        return opMap;
    }

    private JSONObject readJsonFromFile(String path) throws FileNotFoundException, IOException, org.json.simple.parser.ParseException {
        JSONParser parser = new JSONParser();

        Object obj = parser.parse(new FileReader(path));

        JSONObject jsonObject = (JSONObject) obj;

        return jsonObject;
    }

}
