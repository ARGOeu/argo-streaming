/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package flink.jobs.timelines;


import timelines.TimelineAggregator;
import timelines.Utils;
import timelines.Timeline;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
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
public class TimelineAggregatorTest {

    public TimelineAggregatorTest() {
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
     * Test of clear method, of class TimelineAggregator.
     */
    @Test
    public void testClear() {
        System.out.println("clear");
        TimelineAggregator instance = new TimelineAggregator();
        instance.clear();
        // TODO review the generated test code and remove the default call to fail.

    }

    /**
     * Test of tsFromDate method, of class TimelineAggregator.
     */
//    @Test
//    public void testTsFromDate() {
//        System.out.println("tsFromDate");
//        String date = "";
//        TimelineAggregator instance = new TimelineAggregator();
//        String expResult = "";
//        String result = instance.tsFromDate(date);
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//
//    }
    /**
     * Test of createTimeline method, of class TimelineAggregator.
     */
    @Test
    public void testCreateTimeline() throws ParseException {
        System.out.println("createTimeline");
        String name = "test";
        String timestamp = Utils.convertDateToString("yyyy-MM-dd'T'HH:mm:ss'Z'", Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 31, 12, 50, 0));
        int prevState = 0;
        TimelineAggregator instance = new TimelineAggregator();
        instance.createTimeline(name, timestamp, prevState);
        HashMap<String, Timeline> expRes = new HashMap<>();
        Timeline exptimeline = new Timeline(timestamp);
        exptimeline.insert(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 31, 0, 0, 0), 0);
        expRes.put(name, exptimeline);

        assertEquals(expRes.toString(), instance.getInputs().toString());
        // TODO review the generated test code and remove the default call to fail.

    }

    /**
     * Test of insert method, of class TimelineAggregator.
     */
    @Test
    public void testInsert() throws ParseException {
        System.out.println("insert");
        String name = "test";
        String timestamp = Utils.convertDateToString("yyyy-MM-dd'T'HH:mm:ss'Z'", Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 31, 12, 50, 0));

        int status = 0;
        TimelineAggregator instance = new TimelineAggregator();
        instance.insert(name, timestamp, status);
        HashMap<String, Timeline> expRes = new HashMap<>();
        Timeline exptimeline = new Timeline(timestamp);
        exptimeline.insert(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 31, 0, 0, 0), 0);
        expRes.put(name, exptimeline);

        assertEquals(expRes.toString(), instance.getInputs().toString());

        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of setFirst method, of class TimelineAggregator.
     */
    @Test
    public void testSetFirst() throws ParseException {
        System.out.println("setFirst");
        String name = "test1";
        String timestamp = Utils.convertDateToString("yyyy-MM-dd'T'HH:mm:ss'Z'", Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 31, 12, 50, 0));
        String name2 = "test2";
        String timestamp2 = Utils.convertDateToString("yyyy-MM-dd'T'HH:mm:ss'Z'", Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 31, 21, 50, 0));
        HashMap<String, Timeline> map = new HashMap();
        map.put(name, new Timeline(timestamp));
        map.put(name2, new Timeline(timestamp2));

        int status = 0;
        TimelineAggregator instance = new TimelineAggregator(map);
        instance.insert(name, timestamp, status);
        instance.setFirst(name2, timestamp2, status);
        // TODO review the generated test code and remove the default call to fail.

        HashMap<String, Timeline> expRes = new HashMap<>();
        Timeline exptimeline = new Timeline(timestamp);
        exptimeline.insert(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 31, 0, 0, 0), 0);
        Timeline exptimeline2 = new Timeline(timestamp);

        exptimeline2.insert(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 31, 12, 50, 0), 0);
        expRes.put(name2, exptimeline);
        expRes.put(name, exptimeline2);

        assertEquals(expRes, instance.getInputs());
    }

    /**
     * Test of getDate method, of class TimelineAggregator.
     */
    @Test
    public void testGetDate() throws ParseException {
        System.out.println("getDate");
        String name = "test1";
        String timestamp = Utils.convertDateToString("yyyy-MM-dd'T'HH:mm:ss'Z'", Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 31, 12, 50, 0));
        int status = 0;
        TimelineAggregator instance = new TimelineAggregator(timestamp);
        instance.insert(name, timestamp, status);

        LocalDate expResult = Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 31, 12, 50, 0).toLocalDate();
        LocalDate result = instance.getDate();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getSamples method, of class TimelineAggregator.
     */
    @Test
    public void testGetSamples() throws ParseException {
        System.out.println("getSamples");
        String name = "test1";
        String timestamp = Utils.convertDateToString("yyyy-MM-dd'T'HH:mm:ss'Z'", Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 31, 12, 50, 0));
        String name2 = "test2";
        String timestamp2 = Utils.convertDateToString("yyyy-MM-dd'T'HH:mm:ss'Z'", Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 31, 21, 50, 0));
        HashMap<String, Timeline> map = new HashMap();
        map.put(name, new Timeline(timestamp));
        map.put(name2, new Timeline(timestamp2));

        TimelineAggregator instance = new TimelineAggregator(map);
        instance.aggregate(createTruthTable(), 0);
        TreeMap<DateTime, Integer> expRes = new TreeMap<>();
        Timeline exptimeline = new Timeline();
        exptimeline.insert(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 31, 12, 50, 0), 0);
        Set<Map.Entry<DateTime, Integer>> expResult = expRes.entrySet();
        Set<Map.Entry<DateTime, Integer>> result = instance.getSamples();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of clearAndSetDate method, of class TimelineAggregator.
     */
//    @Test
//    public void testClearAndSetDate() {
//        System.out.println("clearAndSetDate");
//        String timestamp = "";
//        TimelineAggregator instance = new TimelineAggregator();
//        instance.clearAndSetDate(timestamp);
//        // TODO review the generated test code and remove the default call to fail.
//        
//    }
    /**
     * Test of aggregate method, of class TimelineAggregator.
     */
    @Test
    public void testAggregate() throws IOException, FileNotFoundException, org.json.simple.parser.ParseException, ParseException {
        System.out.println("aggregate");
        TimelineUtils timelineUtils = new TimelineUtils();
        TimelineUtils.TimelineJson timelinejson = timelineUtils.readTimelines();

        ArrayList<TreeMap> inputTimelines = timelinejson.getInputTimelines();
        int op = timelinejson.getOperation();
        int[][][] truthTable = timelinejson.getTruthTable();
        ArrayList<String> states = timelinejson.getStates();

        TimelineAggregator instance = new TimelineAggregator();

        HashMap<String, Timeline> inputs = new HashMap();
        int counter = 1;
        for (TreeMap<DateTime, Integer> map : inputTimelines) {
            Timeline timeline = new Timeline();
            checkForMissingMidnightStatus(map, states.indexOf("MISSING"));

            timeline.insertDateTimeStamps(map);
            inputs.put(timeline + "_" + counter, timeline);
            counter++;
        }
        instance.setInputs(inputs);

        instance.aggregate(truthTable, op);

        Set<Entry<DateTime, Integer>> expRes = timelinejson.getOutputTimeline().entrySet();
        Set<Entry<DateTime, Integer>> res = instance.getOutput().getSamples();
        assertEquals(expRes, res);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of getOutput method, of class TimelineAggregator.
     */
    @Test
    public void testGetOutput() {
        System.out.println("getOutput");
        TimelineAggregator instance = new TimelineAggregator();
        Timeline expResult = null;
        Timeline result = instance.getOutput();
        //assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of setOutput method, of class TimelineAggregator.
     */
    @Test
    public void testSetOutput() {
        System.out.println("setOutput");
        Timeline output = null;
        TimelineAggregator instance = new TimelineAggregator();
        instance.setOutput(output);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of getInputs method, of class TimelineAggregator.
     */
    @Test
    public void testGetInputs() {
        System.out.println("getInputs");
        TimelineAggregator instance = new TimelineAggregator();
        Map<String, Timeline> expResult = null;
        Map<String, Timeline> result = instance.getInputs();
//        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of setInputs method, of class TimelineAggregator.
     */
    @Test
    public void testSetInputs() {
        System.out.println("setInputs");
        Map<String, Timeline> inputs = null;
        TimelineAggregator instance = new TimelineAggregator();
        instance.setInputs(inputs);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    private int[][][] createTruthTable() {

        int[][][] truthtable = new int[2][6][6];

        truthtable[0][0][0] = 0;
        truthtable[0][0][1] = 0;
        truthtable[0][0][2] = 0;
        truthtable[0][0][3] = 0;
        truthtable[0][0][4] = 0;
        truthtable[0][0][5] = 0;

        truthtable[0][1][0] = -1;
        truthtable[0][1][1] = 1;
        truthtable[0][1][2] = 1;
        truthtable[0][1][3] = 1;
        truthtable[0][1][4] = 1;
        truthtable[0][1][5] = 1;

        truthtable[0][2][0] = -1;
        truthtable[0][2][1] = -1;
        truthtable[0][2][2] = 2;
        truthtable[0][2][3] = 2;
        truthtable[0][2][4] = 4;
        truthtable[0][2][5] = 2;

        truthtable[0][3][0] = -1;
        truthtable[0][3][1] = -1;
        truthtable[0][3][2] = -1;
        truthtable[0][3][3] = 3;
        truthtable[0][3][4] = 4;
        truthtable[0][3][5] = 5;

        truthtable[0][4][0] = -1;
        truthtable[0][4][1] = -1;
        truthtable[0][4][2] = -1;
        truthtable[0][4][3] = -1;
        truthtable[0][4][4] = 4;
        truthtable[0][4][5] = 5;

        truthtable[0][5][0] = -1;
        truthtable[0][5][1] = -1;
        truthtable[0][5][2] = -1;
        truthtable[0][5][3] = -1;
        truthtable[0][5][4] = -1;
        truthtable[0][5][5] = 5;

        truthtable[1][0][0] = 0;
        truthtable[1][0][1] = 1;
        truthtable[1][0][2] = 2;
        truthtable[1][0][3] = 3;
        truthtable[1][0][4] = 4;
        truthtable[1][0][5] = 5;

        truthtable[1][1][0] = -1;
        truthtable[1][1][1] = 1;
        truthtable[1][1][2] = 2;
        truthtable[1][1][3] = 3;
        truthtable[1][1][4] = 4;
        truthtable[1][1][5] = 5;

        truthtable[1][2][0] = -1;
        truthtable[1][2][1] = -1;
        truthtable[1][2][2] = 2;
        truthtable[1][2][3] = 3;
        truthtable[1][2][4] = 4;
        truthtable[1][2][5] = 5;

        truthtable[1][3][0] = -1;
        truthtable[1][3][1] = -1;
        truthtable[1][3][2] = -1;
        truthtable[1][3][3] = 3;
        truthtable[1][3][4] = 4;
        truthtable[1][3][5] = 5;

        truthtable[1][4][0] = -1;
        truthtable[1][4][1] = -1;
        truthtable[1][4][2] = -1;
        truthtable[1][4][3] = -1;
        truthtable[1][4][4] = 4;
        truthtable[1][4][5] = 4;

        truthtable[1][5][0] = -1;
        truthtable[1][5][1] = -1;
        truthtable[1][5][2] = -1;
        truthtable[1][5][3] = -1;
        truthtable[1][5][4] = -1;
        truthtable[1][5][5] = 5;

        return truthtable;

    }

    private void checkForMissingMidnightStatus(TreeMap<DateTime, Integer> map, int missingStatus) throws ParseException {
        DateTime midnight = Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 0, 0, 00);
        if (!map.containsKey(midnight)) {
            map.put(midnight, missingStatus);
        }
    }

    private TreeMap<DateTime, Integer> buildTimeline1() throws ParseException {
        System.out.println("timeline 1 :");
        TreeMap<DateTime, Integer> map = new TreeMap();
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 0, 0, 00), 0);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 0, 15, 50), 1);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 1, 15, 50), 1);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 2, 15, 50), 0);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 3, 15, 50), 1);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 15, 15, 50), 0);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 20, 16, 50), 1);
        for (Entry<DateTime, Integer> entry : map.entrySet()) {
            System.out.println(entry.getKey() + " --- " + entry.getValue());
        }
        return map;
    }

    private TreeMap<DateTime, Integer> buildTimeline2() throws ParseException {
        System.out.println("timeline 2 :");
        TreeMap<DateTime, Integer> map = new TreeMap();
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 0, 0, 00), 0);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 0, 5, 00), 0);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 12, 0, 00), 1);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 14, 0, 00), 0);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 23, 05, 00), 1);
        for (Entry<DateTime, Integer> entry : map.entrySet()) {
            System.out.println(entry.getKey() + " --- " + entry.getValue());
        }
        return map;
    }

    private TreeMap<DateTime, Integer> buildTimeline3() throws ParseException {
        System.out.println("timeline 3 :");
        TreeMap<DateTime, Integer> map = new TreeMap();
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 0, 0, 00), 0);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 0, 5, 00), 2);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 2, 0, 00), 1);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 3, 0, 00), 0);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 6, 00, 00), 0);
        for (Entry<DateTime, Integer> entry : map.entrySet()) {
            System.out.println(entry.getKey() + " --- " + entry.getValue());
        }
        return map;
    }

    private TreeMap<DateTime, Integer> buildTimeline4() throws ParseException {
        System.out.println("timeline 4 :");
        TreeMap<DateTime, Integer> map = new TreeMap();
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 0, 0, 00), 0);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 20, 0, 00), 4);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 21, 0, 00), 0);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 22, 0, 00), 4);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 23, 00, 00), 0);
        for (Entry<DateTime, Integer> entry : map.entrySet()) {
            System.out.println(entry.getKey() + " --- " + entry.getValue());
        }
        return map;
    }

    private TreeMap<DateTime, Integer> buildAggregated() throws ParseException {
        System.out.println("timeline all :");
        TreeMap<DateTime, Integer> map = new TreeMap();

        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 0, 0, 00), 0);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 0, 5, 00), 2);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 2, 0, 00), 1);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 3, 0, 00), 0);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 3, 15, 50), 1);

        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 15, 15, 50), 0);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 20, 0, 00), 4);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 21, 0, 00), 1);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 22, 0, 00), 4);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 23, 00, 00), 1);

        for (Entry<DateTime, Integer> entry : map.entrySet()) {
            System.out.println(entry.getKey() + " --- " + entry.getValue());
        }
        return map;
    }

    private TreeMap<DateTime, Integer> buildAggregated2() throws ParseException {

        TreeMap<DateTime, Integer> map = new TreeMap();

        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 0, 0, 00), 0);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 0, 5, 00), 2);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 2, 0, 00), 1);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 3, 0, 00), 0);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 3, 15, 50), 1);

        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 15, 15, 50), 0);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 20, 0, 00), 4);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 21, 0, 00), 1);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 22, 0, 00), 4);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 23, 00, 00), 1);
        return map;
    }
}
