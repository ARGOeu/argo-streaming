/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.pojos;

import argo.utils.EnumStatus;
import argo.utils.Utils;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.TreeMap;
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
 String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";

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
     * Test of getTimelineMap method, of class Timeline.
     */
    @Test
    public void testGetTimelineMap() throws ParseException {
        System.out.println("getTimelineMap");
        TreeMap<Date, String> testTimelines = new TreeMap<>();
       // String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";

        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 0, 12, 23), EnumStatus.CRITICAL.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 1, 5, 10), EnumStatus.CRITICAL.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 5, 20, 15), EnumStatus.CRITICAL.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 4, 31, 1), EnumStatus.CRITICAL.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 3, 50, 4), EnumStatus.OK.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 21, 3, 5), EnumStatus.OK.name());

        Timeline instance = new Timeline(testTimelines);

        assertEquals(testTimelines, instance.getTimelineMap());
        // TODO review the generated test code and remove the default call to fail.
        //   fail("The test case is a prototype.");
    }

    /**
     * Test of setTimelineMap method, of class Timeline.
     */
    @Test
    public void testSetTimelineMap() throws ParseException {
        System.out.println("setTimelineMap");
        TreeMap<Date, String> timelineMap = new TreeMap<>();
        String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";

        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 0, 12, 23), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 1, 5, 10), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 5, 20, 15), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 4, 31, 1), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 3, 50, 4), EnumStatus.OK.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 21, 3, 5), EnumStatus.OK.name());

        Timeline instance = new Timeline();
        instance.setTimelineMap(timelineMap);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }
//

    /**
     * Test of addTimestamp method, of class Timeline.
     */
    @Test
    public void testAddTimestamp_Date_String() throws ParseException {
        System.out.println("addTimestamp");
   
        Date date = Utils.createDate(format, 2021, 0, 15, 12, 5, 11);
        String status = EnumStatus.OK.name();

        Timeline instance = new Timeline();
        instance.addTimestamp(date, status);

        // TODO review the generated test code and remove the default call to fail.
        TreeMap<Date, String> testMap = new TreeMap<>();
        testMap.put(date, status);
        assertEquals(testMap, instance.getTimelineMap());

        // fail("The test case is a prototype.");
    }

    /**
     * Test of addTimestamp method, of class Timeline.
     */
    @Test
    public void testAddTimestamp_String_String() throws Exception {
        System.out.println("addTimestamp");
        
        String date = "2021-01-15T20:10:03Z";
        
        String status = EnumStatus.OK.name();
        Timeline instance = new Timeline();
        instance.addTimestamp(date, status);
        // TODO review the generated test code and remove the default call to fail.
        TreeMap<Date, String> testMap = new TreeMap<>();
        
        testMap.put(Utils.convertStringtoDate(format, date), status);
        assertEquals(testMap, instance.getTimelineMap());

        //   fail("The test case is a prototype.");
    }

    /**
     * Test of retrieveStatus method, of class Timeline.
     */
    @Test
    public void testRetrieveStatus() throws Exception {
        System.out.println("retrieveStatus");
        String dateStr = "2021-01-15T20:10:03Z";
   
        TreeMap<Date, String> timelineMap = new TreeMap<>();
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 0, 12, 23), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 1, 5, 10), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 3, 50, 4), EnumStatus.OK.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 4, 31, 1), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 5, 20, 15), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 21, 3, 5), EnumStatus.OK.name());

        Timeline instance = new Timeline(timelineMap);

        String expResult = EnumStatus.CRITICAL.name();
        String result = instance.retrieveStatus(dateStr);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }
//

    /**
     * Test of collectTimestampsInTimelineStringFormat method, of class
     * Timeline.
     */
    @Test
    public void testCollectTimestampsInTimelineStringFormat() throws Exception {
        System.out.println("collectTimestampsInTimelineStringFormat");
        TreeMap<Date, String> timelineMap = new TreeMap<>();
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 0, 12, 23), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 1, 5, 10), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 3, 50, 4), EnumStatus.OK.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 4, 31, 1), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 5, 20, 15), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 21, 3, 5), EnumStatus.OK.name());

        Timeline instance = new Timeline(timelineMap);

        ArrayList<String> expResult = new ArrayList<>();
        expResult.add("2021-01-15T00:12:23Z");
        expResult.add("2021-01-15T01:05:10Z");
        expResult.add("2021-01-15T03:50:04Z");
        expResult.add("2021-01-15T04:31:01Z");
        expResult.add("2021-01-15T05:20:15Z");
        expResult.add("2021-01-15T21:03:05Z");

        ArrayList<String> result = instance.collectTimestampsInTimelineStringFormat();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }
//

    /**
     * Test of collectTimestampsInTimeline method, of class Timeline.
     */
    @Test
    public void testCollectTimestampsInTimeline() throws ParseException {
        System.out.println("collectTimestampsInTimeline");
        TreeMap<Date, String> timelineMap = new TreeMap<>();
        
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 0, 12, 23), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 1, 5, 10), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 3, 50, 4), EnumStatus.OK.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 4, 31, 1), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 5, 20, 15), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 21, 3, 5), EnumStatus.OK.name());

        Timeline instance = new Timeline(timelineMap);

        ArrayList<Date> expResult = new ArrayList();

        expResult.add(Utils.createDate(format, 2021, 0, 15, 0, 12, 23));
        expResult.add(Utils.createDate(format, 2021, 0, 15, 1, 5, 10));
        expResult.add(Utils.createDate(format, 2021, 0, 15, 3, 50, 4));
        expResult.add(Utils.createDate(format, 2021, 0, 15, 4, 31, 1));
        expResult.add(Utils.createDate(format, 2021, 0, 15, 5, 20, 15));
        expResult.add(Utils.createDate(format, 2021, 0, 15, 21, 3, 5));

        ArrayList<Date> result = instance.collectTimestampsInTimeline();
        assertEquals(expResult.toString(), result.toString());
        // TODO review the generated test code and remove the default call to fail.
        //   fail("The test case is a prototype.");
    }
//

    /**
     * Test of calculateStatusChanges method, of class Timeline.
     */
    @Test
    public void testCalculateStatusChanges() throws ParseException {
        System.out.println("calculateStatusChanges");
        TreeMap<Date, String> timelineMap = new TreeMap<>();
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 0, 12, 23), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 1, 5, 10), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 3, 50, 4), EnumStatus.OK.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 4, 31, 1), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 5, 20, 15), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 21, 3, 5), EnumStatus.OK.name());

        Timeline instance = new Timeline(timelineMap);

        int expResult = 3;
        int result = instance.calculateStatusChanges();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
    }
//

    /**
     * Test of manageFirstLastTimestamps method, of class Timeline.
     */
    @Test
    public void testManageFirstLastTimestamps() throws Exception {
        System.out.println("manageFirstLastTimestamps");
  String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        TreeMap<Date, String> timelineMap = new TreeMap<>();
        timelineMap.put(Utils.createDate(format, 2021, 0, 14, 23, 12, 23), EnumStatus.OK.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 0, 12, 23), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 1, 5, 10), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 3, 50, 4), EnumStatus.OK.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 4, 31, 1), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 5, 20, 15), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 21, 3, 5), EnumStatus.OK.name());

        Timeline instance = new Timeline(timelineMap);

        instance.manageFirstLastTimestamps();

        TreeMap<Date, String> expResult = new TreeMap<>();
        expResult.put(Utils.createDate(format, 2021, 0, 15, 0, 0, 0), EnumStatus.OK.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 0, 12, 23), EnumStatus.CRITICAL.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 1, 5, 10), EnumStatus.CRITICAL.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 3, 50, 4), EnumStatus.OK.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 4, 31, 1), EnumStatus.CRITICAL.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 5, 20, 15), EnumStatus.CRITICAL.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 21, 3, 5), EnumStatus.OK.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 23, 59, 59), EnumStatus.OK.name());

        TreeMap<Date, String> result = instance.getTimelineMap();
        assertEquals(expResult.toString(), result.toString());

        
        
        timelineMap = new TreeMap<>();
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 0, 12, 23), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 1, 5, 10), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 3, 50, 4), EnumStatus.OK.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 4, 31, 1), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 5, 20, 15), EnumStatus.CRITICAL.name());
        timelineMap.put(Utils.createDate(format, 2021, 0, 15, 21, 3, 5), EnumStatus.OK.name());

        instance = new Timeline(timelineMap);

        instance.manageFirstLastTimestamps();

        expResult = new TreeMap<>();
        expResult.put(Utils.createDate(format, 2021, 0, 15, 0, 0, 0), EnumStatus.MISSING.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 0, 12, 23), EnumStatus.CRITICAL.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 1, 5, 10), EnumStatus.CRITICAL.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 3, 50, 4), EnumStatus.OK.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 4, 31, 1), EnumStatus.CRITICAL.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 5, 20, 15), EnumStatus.CRITICAL.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 21, 3, 5), EnumStatus.OK.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 23, 59, 59), EnumStatus.OK.name());

        result = instance.getTimelineMap();
        assertEquals(expResult.toString(), result.toString());

        
// TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }
}
