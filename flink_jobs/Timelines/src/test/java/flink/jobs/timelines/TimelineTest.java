/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package flink.jobs.timelines;

import timelines.Utils;
import timelines.Timeline;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
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
public class TimelineTest {

    public TimelineTest() {
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
     * Test of get method, of class Timeline.
     */
    @Test
    public void testGet_String() throws ParseException {
        System.out.println("get");

        DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
        DateTime timestamp = Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 4, 31, 1);
        String timestampStr = timestamp.toString(dtf);
        Timeline instance = new Timeline();
        instance.insertDateTimeStamps(createTimestampList());
        int expResult = -1;
        int result = instance.get(timestampStr);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of get method, of class Timeline.
     */
    @Test
    public void testGet_DateTime() throws ParseException {
        System.out.println("get");
        DateTime point = Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 4, 31, 1);
        Timeline instance = new Timeline();
        instance.insertDateTimeStamps(createTimestampList());
        int expResult = 1;
        int result = instance.get(point);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of insert method, of class Timeline.
     */
    @Test
    public void testInsert_String_int() throws ParseException {
        System.out.println("insert");
        DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

        DateTime timestamp = Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 4, 31, 1);
        String timestampStr = timestamp.toString(dtf);

        int status = 1;
        Timeline instance = new Timeline();
        instance.insert(timestampStr, status);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of insert method, of class Timeline.
     */
    @Test
    public void testInsert_DateTime_int() throws ParseException {
        System.out.println("insert");
        DateTime timestamp = Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 4, 31, 1);

        int status = 0;
        Timeline instance = new Timeline();
        instance.insert(timestamp, status);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of insertStringTimeStamps method, of class Timeline.
     */
    @Test
    public void testInsertStringTimeStamps() throws ParseException {
        System.out.println("insertStringTimeStamps");
        TreeMap<String, Integer> timestamps = createStringTimestampList();
        Timeline instance = new Timeline();
        instance.insertStringTimeStamps(timestamps);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of insertDateTimeStamps method, of class Timeline.
     */
    @Test
    public void testInsertDateTimeStamps() throws ParseException {
        System.out.println("insertDateTimeStamps");
        TreeMap<DateTime, Integer> timestamps = createTimestampList();
        Timeline instance = new Timeline();
        instance.insertDateTimeStamps(timestamps);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of setFirst method, of class Timeline.
     */
    @Test
    public void testSetFirst() throws ParseException {
        System.out.println("setFirst");
        DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

        DateTime timestamp = Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 0, 0, 0, 1);
        String timestampStr = timestamp.toString(dtf);

        int state = 0;
        Timeline instance = new Timeline();
        instance.setFirst(timestampStr, state);
        // TODO review the generated test code and remove the default call to fail.
        //   fail("The test case is a prototype.");
    }

    /**
     * Test of clear method, of class Timeline.
     */
    @Test
    public void testClear() throws ParseException {
        System.out.println("clear");
        Timeline instance = new Timeline();
        instance.insertDateTimeStamps(createTimestampList());
        instance.clear();
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of bulkInsert method, of class Timeline.
     */
    @Test
    public void testBulkInsert() throws ParseException {
        System.out.println("bulkInsert");
        Set<Map.Entry<DateTime, Integer>> samples = createTimestampList().entrySet();
        Timeline instance = new Timeline();
        instance.bulkInsert(samples);
        // TODO review the generated test code and remove the default call to fail.
        //   fail("The test case is a prototype.");
    }

    /**
     * Test of getSamples method, of class Timeline.
     */
    @Test
    public void testGetSamples() throws ParseException {
        System.out.println("getSamples");
        Timeline instance = new Timeline();
        instance.insertDateTimeStamps(createTimestampList());
        Set<Map.Entry<DateTime, Integer>> expResult = instance.getSamples();
        Set<Map.Entry<DateTime, Integer>> result = instance.getSamples();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getDate method, of class Timeline.
     */
    @Test
    public void testGetDate() throws ParseException {
        System.out.println("getDate");
        DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

        Timeline instance = new Timeline(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 1, 0, 0, 0, 0).toString(dtf));

        LocalDate expResult = new LocalDate(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 1, 0, 0, 0, 0));
        LocalDate result = instance.getDate();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getLength method, of class Timeline.
     */
    @Test
    public void testGetLength() throws ParseException {
        System.out.println("getLength");
        Timeline instance = new Timeline();
        instance.insertDateTimeStamps(createTimestampList());
        int expResult = 2;
        int result = instance.getLength();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of isEmpty method, of class Timeline.
     */
    @Test
    public void testIsEmpty() throws ParseException {
        System.out.println("isEmpty");
        Timeline instance = new Timeline();
        instance.insertDateTimeStamps(createTimestampList());

        boolean expResult = false;
        boolean result = instance.isEmpty();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of optimize method, of class Timeline.
     */
    @Test
    public void testOptimize() throws ParseException {
        System.out.println("optimize");
        Timeline instance = new Timeline();
        instance.insertDateTimeStamps(createTimestampList());
        instance.optimize();
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getPoints method, of class Timeline.
     */
    @Test
    public void testGetPoints() throws ParseException {
        System.out.println("getPoints");
        Timeline instance = new Timeline();
        TreeMap<DateTime, Integer> map = createTimestampList();
        instance.insertDateTimeStamps(map);
        Set<DateTime> expResult=new TreeSet<>();
        expResult.add(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 5, 20, 15));
        expResult.add(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 0, 12, 23));

        Set<DateTime> result = instance.getPoints();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //   fail("The test case is a prototype.");
    }

    /**
     * Test of aggregate method, of class Timeline.
     */
    @Test
    public void testAggregate() throws ParseException {
        System.out.println("aggregate");
        Timeline second = new Timeline();
        second.insertDateTimeStamps(createSecondTimeline());
        int[][][] truthTable = createTruthTable();
        int op = 0;
        Timeline instance = new Timeline();
        instance.aggregate(second, truthTable, op);
        Set<Map.Entry<DateTime, Integer>> expResult = createMerged().entrySet();
        Set<Map.Entry<DateTime, Integer>> result = instance.getSamples();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of buildStringTimeStampMap method, of class Timeline.
     */
//    @Test
//    public void testBuildStringTimeStampMap() {
//        System.out.println("buildStringTimeStampMap");
//        ArrayList timestampList = null;
//        OperationsParser op = null;
//        Timeline instance = new Timeline();
//        TreeMap<String, Integer> expResult = null;
//        TreeMap<String, Integer> result = instance.buildStringTimeStampMap(timestampList, op);
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        //fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of buildDateTimeStampMap method, of class Timeline.
//     */
//    @Test
//    public void testBuildDateTimeStampMap() {
//        System.out.println("buildDateTimeStampMap");
//        ArrayList timestampList = null;
//        OperationsParser op = null;
//        Timeline instance = new Timeline();
//        TreeMap<DateTime, Integer> expResult = null;
//        TreeMap<DateTime, Integer> result = instance.buildDateTimeStampMap(timestampList, op);
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of removeTimeStamp method, of class Timeline.
//     */
//    @Test
//    public void testRemoveTimeStamp() {
//        System.out.println("removeTimeStamp");
//        DateTime timestamp = null;
//        Timeline instance = new Timeline();
//        instance.removeTimeStamp(timestamp);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
    /**
     * Test of calcStatusChanges method, of class Timeline.
     */
    @Test
    public void testCalcStatusChanges() throws ParseException {
        System.out.println("calcStatusChanges");
        Timeline instance = new Timeline();
        instance.insertDateTimeStamps(createTimestampList());
        int expResult = 1;
        int result = instance.calcStatusChanges();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of replacePreviousDateStatus method, of class Timeline.
     */
    @Test
    public void testReplacePreviousDateStatus() throws ParseException {
        System.out.println("replacePreviousDateStatus");
        DateTime date = Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 0, 0, 0);
        Timeline instance = new Timeline();
        instance.insertDateTimeStamps(createTimestampList());
        ArrayList<String> availStates = new ArrayList<>();
        availStates.add("OK");
        availStates.add("WARNING");
        availStates.add("UKNOWN");
        availStates.add("MISSING");
        availStates.add("CRITICAL");
        availStates.add("DOWNTIME");

        instance.replacePreviousDateStatus(date, availStates);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of hashCode method, of class Timeline.
     */
//    @Test
//    public void testHashCode() {
//        System.out.println("hashCode");
//        Timeline instance = new Timeline();
//        int expResult = 0;
//        int result = instance.hashCode();
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
    /**
     * Test of equals method, of class Timeline.
     */
//    @Test
//    public void testEquals() {
//        System.out.println("equals");
//        Object obj = null;
//        Timeline instance = new Timeline();
//        boolean expResult = false;
//        boolean result = instance.equals(obj);
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
    /**
     * Test of opInt method, of class Timeline.
     */
    @Test
    public void testOpInt() throws ParseException {
        System.out.println("opInt");
        int[][][] truthTable = createTruthTable();
        int op = 0;
        int a = 0;
        int b = 0;
        Timeline instance = new Timeline();
        instance.insertDateTimeStamps(createTimestampList());
        int expResult = 0;
        int result = instance.opInt(truthTable, op, a, b);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    private TreeMap<DateTime, Integer> createTimestampList() throws ParseException {
        TreeMap<DateTime, Integer> map = new TreeMap<>();

        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 0, 12, 23), 1);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 1, 5, 10), 1);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 5, 20, 15), 0);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 4, 31, 1), 1);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 3, 50, 4), 1);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 21, 3, 5), 0);
        return map;
//
    }

    private TreeMap<String, Integer> createStringTimestampList() throws ParseException {
        TreeMap<String, Integer> map = new TreeMap<>();

        DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 0, 12, 23).toString(dtf), 1);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 1, 5, 10).toString(dtf), 1);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 5, 20, 15).toString(dtf), 0);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 4, 31, 1).toString(dtf), 1);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 3, 50, 4).toString(dtf), 1);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 21, 3, 5).toString(dtf), 0);
        return map;
//
    }

    private TreeMap<DateTime, Integer> createSecondTimeline() throws ParseException {
        TreeMap<DateTime, Integer> map = new TreeMap<>();

        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 0, 15, 50), 1);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 2, 5, 10), 0);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 5, 20, 15), 0);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 4, 31, 1), 1);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 3, 50, 4), 1);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 21, 3, 5), 0);
        return map;
//
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

    private TreeMap createMerged() throws ParseException {
        TreeMap<DateTime, Integer> map = new TreeMap();
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 0, 15, 50), 1);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 2, 5, 10), 0);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 3, 50, 4), 1);
        
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 5, 20, 15), 0);
        return map;
    }

}
