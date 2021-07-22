/*s
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package operations;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertArrayEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/*
 * A unit test class to test OperationsManager
 */
public class OperationsManagerTest {

    public OperationsManagerTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        assertNotNull("Test file missing", OperationsManagerTest.class.getResource("/operations/operations.json"));
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
    /**
     * Test of getDefaultDown method, of class OperationsManager.
     */
    @Test
    public void testGetDefaultDown() throws IOException {
        System.out.println("getDefaultDown");
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));
        String expResult = "DOWNTIME";
        String result = instance.getDefaultDown();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getDefaultUnknown method, of class OperationsManager.
     */
    @Test
    public void testGetDefaultUnknown() throws IOException {
        System.out.println("getDefaultUnknown");
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));

        String expResult = "UNKNOWN";
        String result = instance.getDefaultUnknown();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getDefaultUnknownInt method, of class OperationsManager.
     */
    @Test
    public void testGetDefaultUnknownInt() throws IOException {
        System.out.println("getDefaultUnknownInt");
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));

        int expResult = 2;
        int result = instance.getDefaultUnknownInt();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getDefaultDownInt method, of class OperationsManager.
     */
    @Test
    public void testGetDefaultDownInt() throws IOException {
        System.out.println("getDefaultDownInt");
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));

        int expResult = 5;
        int result = instance.getDefaultDownInt();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getDefaultMissing method, of class OperationsManager.
     */
    @Test
    public void testGetDefaultMissing() throws IOException {
        System.out.println("getDefaultMissing");
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));

        String expResult = "MISSING";
        String result = instance.getDefaultMissing();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getDefaultMissingInt method, of class OperationsManager.
     */
    @Test
    public void testGetDefaultMissingInt() throws IOException {
        System.out.println("getDefaultMissingInt");
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));

        int expResult = 3;
        int result = instance.getDefaultMissingInt();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
    }

    /**
     * Test of clear method, of class OperationsManager.
     */
    @Test
    public void testClear() throws IOException {
        System.out.println("clear");
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));

        instance.clear();
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of opInt method, of class OperationsManager.
     */
    @Test
    public void testOpInt_3args_1() throws IOException {
        System.out.println("opInt");
        int op = 0;
        int a = 0;
        int b = 0;
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));

        int expResult = 0;
        int result = instance.opInt(op, a, b);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of opInt method, of class OperationsManager.
     */
    @Test
    public void testOpInt_3args_2() throws IOException {
        System.out.println("opInt");
        String op = "AND";
        String a = "OK";
        String b = "OK";
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));

        int expResult = 0;
        int result = instance.opInt(op, a, b);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of op method, of class OperationsManager.
     */
    @Test
    public void testOp_3args_1() throws IOException {
        System.out.println("op");
        int op = 0;
        int a = 0;
        int b = 0;
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));

        String expResult = "OK";
        String result = instance.op(op, a, b);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of op method, of class OperationsManager.
     */
    @Test
    public void testOp_3args_2() throws IOException {
        System.out.println("op");
        String op = "AND";
        String a = "OK";
        String b = "OK";
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));

        String expResult = "OK";
        String result = instance.op(op, a, b);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getStrStatus method, of class OperationsManager.
     */
    @Test
    public void testGetStrStatus() throws IOException {
        System.out.println("getStrStatus");
        int status = 0;
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));

        String expResult = "OK";
        String result = instance.getStrStatus(status);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getIntStatus method, of class OperationsManager.
     */
    @Test
    public void testGetIntStatus() throws IOException {
        System.out.println("getIntStatus");
        String status = "WARNING";
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));

        int expResult = 1;
        int result = instance.getIntStatus(status);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of getStrOperation method, of class OperationsManager.
     */
    @Test
    public void testGetStrOperation() throws IOException {
        System.out.println("getStrOperation");
        int op = 1;
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));

        String expResult = "OR";
        String result = instance.getStrOperation(op);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getIntOperation method, of class OperationsManager.
     */
    @Test
    public void testGetIntOperation() throws IOException {
        System.out.println("getIntOperation");
        String op = "OR";
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));

        int expResult = 1;
        int result = instance.getIntOperation(op);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //   fail("The test case is a prototype.");
    }

    /**
     * Test of availableStates method, of class OperationsManager.
     */
    @Test
    public void testAvailableStates() throws IOException {
        System.out.println("availableStates");
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));

        ArrayList<String> expResult = new ArrayList();
        expResult.add("OK");
        expResult.add("WARNING");
        expResult.add("UNKNOWN");
        expResult.add("MISSING");
        expResult.add("CRITICAL");
        expResult.add("DOWNTIME");

        ArrayList<String> result = instance.availableStates();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //   fail("The test case is a prototype.");
    }

    /**
     * Test of availableOps method, of class OperationsManager.
     */
    @Test
    public void testAvailableOps() throws IOException {
        System.out.println("availableOps");
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));

        ArrayList<String> expResult = new ArrayList<>();
        expResult.add("AND");
        expResult.add("OR");
        ArrayList<String> result = instance.availableOps();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of loadJson method, of class OperationsManager.
     */
    @Test
    public void testLoadJson() throws Exception {
        System.out.println("loadJson");
        File jsonFile = new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile());
        OperationsManager instance = new OperationsManager();

        instance.loadJson(jsonFile);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of loadJsonString method, of class OperationsManager.
     */
//    @Test
//    public void testLoadJsonString() {
//        System.out.println("loadJsonString");
//        List<String> opsJson = null;
//        OperationsManager instance = new OperationsManager();
//        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));
//
//        instance.loadJsonString(opsJson);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
    /**
     * Test of getTruthTable method, of class OperationsManager.
     */
    @Test
    public void testGetTruthTable() throws IOException, FileNotFoundException, ParseException {
        System.out.println("getTruthTable");
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));
        Utils utils=new Utils();
        
        int[][][] expResult = utils.readTruthTable();
        int[][][] result = instance.getTruthTable();
        assertArrayEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of setTruthTable method, of class OperationsManager.
     */
    @Test
    public void testSetTruthTable() throws IOException, FileNotFoundException, ParseException {
        System.out.println("setTruthTable");
             Utils utils=new Utils();
        
   
        int[][][] truthTable = utils.readTruthTable();
        OperationsManager instance = new OperationsManager();
        instance.setTruthTable(truthTable);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of getStates method, of class OperationsManager.
     */
    @Test
    public void testGetStates() throws IOException {
        System.out.println("getStates");
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));

        HashMap<String, Integer> expResult = new HashMap<>();
        expResult.put("OK", 0);
        expResult.put("WARNING", 1);
        expResult.put("UNKNOWN", 2);
        expResult.put("MISSING", 3);
        expResult.put("CRITICAL", 4);
        expResult.put("DOWNTIME", 5);
        HashMap<String, Integer> result = instance.getStates();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of setStates method, of class OperationsManager.
     */
    @Test
    public void testSetStates() {
        System.out.println("setStates");
        HashMap<String, Integer> states = new HashMap<>();
        states.put("OK", 0);
        states.put("WARNING", 1);
        states.put("UNKNOWN", 2);
        states.put("MISSING", 3);
        states.put("CRITICAL", 4);
        states.put("DOWNTIME", 5);
        OperationsManager instance = new OperationsManager();
        instance.setStates(states);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getOps method, of class OperationsManager.
     */
    @Test
    public void testGetOps() throws IOException {
        System.out.println("getOps");
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));

        HashMap<String, Integer> expResult = new HashMap<>();
        expResult.put("AND", 0);
        expResult.put("OR", 1);
        HashMap<String, Integer> result = instance.getOps();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of setOps method, of class OperationsManager.
     */
    @Test
    public void testSetOps() {
        System.out.println("setOps");
        HashMap<String, Integer> ops = new HashMap<>();
        ops.put("AND", 0);
        ops.put("OR", 1);

        OperationsManager instance = new OperationsManager();
        instance.setOps(ops);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getRevStates method, of class OperationsManager.
     */
    @Test
    public void testGetRevStates() throws IOException {
        System.out.println("getRevStates");
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));

        ArrayList<String> expResult = new ArrayList<>();
        expResult.add("OK");
        expResult.add("WARNING");
        expResult.add("UNKNOWN");
        expResult.add("MISSING");
        expResult.add("CRITICAL");
        expResult.add("DOWNTIME");
        ArrayList<String> result = instance.getRevStates();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of setRevStates method, of class OperationsManager.
     */
    @Test
    public void testSetRevStates() {
        System.out.println("setRevStates");
        ArrayList<String> revStates = new ArrayList<>();
        revStates.add("OK");
        revStates.add("WARNING");
        revStates.add("UNKNWON");
        revStates.add("MISSING");
        revStates.add("CRITICAL");
        revStates.add("DOWNTIME");

        OperationsManager instance = new OperationsManager();
        instance.setRevStates(revStates);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of getRevOps method, of class OperationsManager.
     */
    @Test
    public void testGetRevOps() throws IOException {
        System.out.println("getRevOps");
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));

        ArrayList<String> expResult = new ArrayList<>();
        expResult.add("AND");
        expResult.add("OR");
        ArrayList<String> result = instance.getRevOps();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of setRevOps method, of class OperationsManager.
     */
    @Test
    public void testSetRevOps() {
        System.out.println("setRevOps");
        ArrayList<String> revOps = new ArrayList<>();
        revOps.add("AND");
        revOps.add("OR");
        OperationsManager instance = new OperationsManager();
        instance.setRevOps(revOps);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getDefaultDownState method, of class OperationsManager.
     */
    @Test
    public void testGetDefaultDownState() throws IOException {
        System.out.println("getDefaultDownState");
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));

        String expResult = "DOWNTIME";
        String result = instance.getDefaultDownState();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of setDefaultDownState method, of class OperationsManager.
     */
    @Test
    public void testSetDefaultDownState() {
        System.out.println("setDefaultDownState");
        String defaultDownState = "DOWNTIME";
        OperationsManager instance = new OperationsManager();
        instance.setDefaultDownState(defaultDownState);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getDefaultMissingState method, of class OperationsManager.
     */
    @Test
    public void testGetDefaultMissingState() throws IOException {
        System.out.println("getDefaultMissingState");
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));

        String expResult = "MISSING";
        String result = instance.getDefaultMissingState();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of setDefaultMissingState method, of class OperationsManager.
     */
    @Test
    public void testSetDefaultMissingState() {
        System.out.println("setDefaultMissingState");
        String defaultMissingState = "MISSING";
        OperationsManager instance = new OperationsManager();
        instance.setDefaultMissingState(defaultMissingState);
        // TODO review the generated test code and remove the default call to fail.
        //   fail("The test case is a prototype.");
    }

    /**
     * Test of getDefaultUnknownState method, of class OperationsManager.
     */
    @Test
    public void testGetDefaultUnknownState() throws IOException {
        System.out.println("getDefaultUnknownState");

        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));
        String expResult = "UNKNOWN";
        String result = instance.getDefaultUnknownState();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of setDefaultUnknownState method, of class OperationsManager.
     */
    @Test
    public void testSetDefaultUnknownState() {
        System.out.println("setDefaultUnknownState");
        String defaultUnknownState = "UNKNOWN";
        OperationsManager instance = new OperationsManager();
        instance.setDefaultUnknownState(defaultUnknownState);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of isOrder method, of class OperationsManager.
     */
    @Test
    public void testIsOrder() throws IOException {
        System.out.println("isOrder");
        OperationsManager instance = new OperationsManager();
        instance.loadJson(new File(OperationsManagerTest.class.getResource("/operations/operations.json").getFile()));

        boolean expResult = false;
        boolean result = instance.isOrder();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
    }

    /**
     * Test of setOrder method, of class OperationsManager.
     */
    @Test
    public void testSetOrder() {
        System.out.println("setOrder");
        boolean order = false;
        OperationsManager instance = new OperationsManager();
        instance.setOrder(order);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

}
