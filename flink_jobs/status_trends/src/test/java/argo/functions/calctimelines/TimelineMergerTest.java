/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.functions.calctimelines;

import argo.pojos.Timeline;

import argo.profiles.ProfilesLoader;
import argo.utils.EnumStatus;
import argo.utils.Utils;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.TreeMap;
import org.apache.flink.api.java.utils.ParameterTool;
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
public class TimelineMergerTest {

    String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    public TimelineMergerTest() {
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
     * Test of mergeTimelines method, of class TimelineMerger.
     */
    @Test
    public void testMergeTimelines() throws Exception {
        System.out.println("collectTimestamps");
        HashMap<String, String> map = new HashMap<>();

        map.put("yesterdayData", "hdfs://localhost:9000/user/cthermolia/egi-mdata-2021-01-14");
        map.put("todayData", "hdfs://localhost:9000/user/cthermolia/egi-mdata-0-0");
        map.put("N", "10");
        map.put("mongoUri", "mongodb://localhost:27017/trendsdb");
        map.put("apiUri", "https://api.devel.argo.grnet.gr/api/v2");
        map.put("key", "1dc190a3e0f539c38baab65f1dbfc3ee1d0fee9f");
        map.put("reportId", "04edb428-01e6-4286-87f1-050546736f7c");
        map.put("date", "2021-01-15");
        ParameterTool params = ParameterTool.fromMap(map);

        ProfilesLoader profilesLoader = new ProfilesLoader(params);
        System.out.println("mergeTimelines");
        ArrayList<Timeline> timelineList = new ArrayList();
        timelineList.add(createTimeline1());
        timelineList.add(createTimeline2());
        timelineList.add(createTimeline3());

        TimelineMerger instance = new TimelineMerger(profilesLoader.getAggregationProfileParser().getMetricOp(), profilesLoader.getOperationParser());

        TreeMap<Date, String> expMap = createExpResultFinalTimeline();
        Timeline expResult = new Timeline(expMap);
        Timeline result = instance.mergeTimelines(timelineList);
        assertEquals(expResult.getTimelineMap(), result.getTimelineMap());
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of collectTimestamps method, of class TimelineMerger.
     */
    @Test
    public void testCollectTimestamps() throws IOException, org.json.simple.parser.ParseException, ParseException {
        System.out.println("collectTimestamps");
        HashMap<String, String> map = new HashMap<>();

        map.put("yesterdayData", "hdfs://localhost:9000/user/cthermolia/egi-mdata-2021-01-14");
        map.put("todayData", "hdfs://localhost:9000/user/cthermolia/egi-mdata-0-0");
        map.put("N", "10");
        map.put("mongoUri", "mongodb://localhost:27017/trendsdb");
        map.put("apiUri", "https://api.devel.argo.grnet.gr/api/v2");
        map.put("key", "1dc190a3e0f539c38baab65f1dbfc3ee1d0fee9f");
        map.put("reportId", "04edb428-01e6-4286-87f1-050546736f7c");
        map.put("date", "2021-01-15");
        ParameterTool params = ParameterTool.fromMap(map);

        ProfilesLoader profilesLoader = new ProfilesLoader(params);
        System.out.println("mergeTimelines");
        ArrayList<Timeline> timelineList = new ArrayList();
        timelineList.add(createTimeline1());
        timelineList.add(createTimeline2());
        timelineList.add(createTimeline3());

        TimelineMerger instance = new TimelineMerger(profilesLoader.getAggregationProfileParser().getMetricOp(), profilesLoader.getOperationParser());

        ArrayList<Date> expResult = createTimestampList();
        ArrayList<Date> result = instance.collectTimestamps(timelineList);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of gatherStatusesPerTimestamp method, of class TimelineMerger.
     */
    @Test
    public void testGatherStatusesPerTimestamp() throws ParseException, IOException, org.json.simple.parser.ParseException {
        System.out.println("gatherStatusesPerTimestamp");
        ArrayList<Date> timestamps = createTimestampList();
        HashMap<String, String> map = new HashMap<>();

        map.put("yesterdayData", "hdfs://localhost:9000/user/cthermolia/egi-mdata-2021-01-14");
        map.put("todayData", "hdfs://localhost:9000/user/cthermolia/egi-mdata-0-0");
        map.put("N", "10");
        map.put("mongoUri", "mongodb://localhost:27017/trendsdb");
        map.put("apiUri", "https://api.devel.argo.grnet.gr/api/v2");
        map.put("key", "1dc190a3e0f539c38baab65f1dbfc3ee1d0fee9f");
        map.put("reportId", "04edb428-01e6-4286-87f1-050546736f7c");
        map.put("date", "2021-01-15");
        ParameterTool params = ParameterTool.fromMap(map);

        ProfilesLoader profilesLoader = new ProfilesLoader(params);
        System.out.println("mergeTimelines");
        ArrayList<Timeline> timelineList = new ArrayList();
        timelineList.add(createTimeline1());
        timelineList.add(createTimeline2());
        timelineList.add(createTimeline3());

        TimelineMerger instance = new TimelineMerger(profilesLoader.getAggregationProfileParser().getMetricOp(), profilesLoader.getOperationParser());
        TreeMap<Date, ArrayList<String>> expResult = createTimestampStatusList();
        TreeMap<Date, ArrayList<String>> result = instance.gatherStatusesPerTimestamp(timestamps, timelineList);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    private Timeline createTimeline1() throws ParseException {

        TreeMap<Date, String> testTimelines = new TreeMap<>();
        //   String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 0, 0, 0), EnumStatus.OK.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 0, 12, 23), EnumStatus.CRITICAL.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 1, 5, 10), EnumStatus.CRITICAL.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 5, 20, 15), EnumStatus.CRITICAL.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 4, 31, 1), EnumStatus.CRITICAL.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 3, 50, 4), EnumStatus.OK.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 21, 3, 5), EnumStatus.OK.name());

        Timeline instance = new Timeline(testTimelines);
        return instance;
    }

    private Timeline createTimeline2() throws ParseException {

        TreeMap<Date, String> testTimelines = new TreeMap<>();
        //   String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 0, 0, 0), EnumStatus.CRITICAL.name());

        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 0, 12, 23), EnumStatus.CRITICAL.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 1, 5, 10), EnumStatus.CRITICAL.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 5, 20, 15), EnumStatus.CRITICAL.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 4, 31, 1), EnumStatus.CRITICAL.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 12, 50, 4), EnumStatus.WARNING.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 16, 30, 4), EnumStatus.WARNING.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 22, 3, 5), EnumStatus.OK.name());

        Timeline instance = new Timeline(testTimelines);
        return instance;
    }

    private Timeline createTimeline3() throws ParseException {

        TreeMap<Date, String> testTimelines = new TreeMap<>();
        //   String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 0, 0, 0), EnumStatus.CRITICAL.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 0, 50, 23), EnumStatus.CRITICAL.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 1, 5, 10), EnumStatus.CRITICAL.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 5, 20, 15), EnumStatus.OK.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 4, 31, 1), EnumStatus.CRITICAL.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 12, 50, 4), EnumStatus.WARNING.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 16, 30, 4), EnumStatus.WARNING.name());
        testTimelines.put(Utils.createDate(format, 2021, 0, 15, 22, 3, 5), EnumStatus.OK.name());

        Timeline instance = new Timeline(testTimelines);
        return instance;
    }

    private ArrayList<Date> createTimestampList() throws ParseException {

        ArrayList<Date> expResult = new ArrayList<>();
        expResult.add(Utils.createDate(format, 2021, 0, 15, 0, 0, 0));
        expResult.add(Utils.createDate(format, 2021, 0, 15, 0, 12, 23));
        expResult.add(Utils.createDate(format, 2021, 0, 15, 0, 50, 23));
        expResult.add(Utils.createDate(format, 2021, 0, 15, 1, 5, 10));
        expResult.add(Utils.createDate(format, 2021, 0, 15, 3, 50, 4));
        expResult.add(Utils.createDate(format, 2021, 0, 15, 4, 31, 1));
        expResult.add(Utils.createDate(format, 2021, 0, 15, 5, 20, 15));
        expResult.add(Utils.createDate(format, 2021, 0, 15, 12, 50, 4));
        expResult.add(Utils.createDate(format, 2021, 0, 15, 16, 30, 4));
        expResult.add(Utils.createDate(format, 2021, 0, 15, 21, 3, 5));
        expResult.add(Utils.createDate(format, 2021, 0, 15, 22, 3, 5));
        return expResult;
    }

    private TreeMap<Date, ArrayList<String>> createTimestampStatusList() throws ParseException {
        TreeMap<Date, ArrayList<String>> expResult = new TreeMap<>();

        ArrayList<String> list1 = new ArrayList();
        list1.add(EnumStatus.OK.name());
        list1.add(EnumStatus.CRITICAL.name());
        list1.add(EnumStatus.CRITICAL.name());

        ArrayList<String> list2 = new ArrayList();
        list2.add(EnumStatus.CRITICAL.name());
        list2.add(EnumStatus.CRITICAL.name());
        list2.add(EnumStatus.CRITICAL.name());

        ArrayList<String> list3 = new ArrayList();
        list3.add(EnumStatus.CRITICAL.name());
        list3.add(EnumStatus.CRITICAL.name());
        list3.add(EnumStatus.CRITICAL.name());

        ArrayList<String> list4 = new ArrayList();
        list4.add(EnumStatus.CRITICAL.name());
        list4.add(EnumStatus.CRITICAL.name());
        list4.add(EnumStatus.CRITICAL.name());

        ArrayList<String> list5 = new ArrayList();
        list5.add(EnumStatus.OK.name());
        list5.add(EnumStatus.CRITICAL.name());
        list5.add(EnumStatus.CRITICAL.name());

        ArrayList<String> list6 = new ArrayList();
        list6.add(EnumStatus.CRITICAL.name());
        list6.add(EnumStatus.CRITICAL.name());
        list6.add(EnumStatus.CRITICAL.name());

        ArrayList<String> list7 = new ArrayList();
        list7.add(EnumStatus.CRITICAL.name());
        list7.add(EnumStatus.CRITICAL.name());
        list7.add(EnumStatus.OK.name());

        ArrayList<String> list8 = new ArrayList();
        list8.add(EnumStatus.CRITICAL.name());
        list8.add(EnumStatus.WARNING.name());
        list8.add(EnumStatus.WARNING.name());

        ArrayList<String> list9 = new ArrayList();
        list9.add(EnumStatus.OK.name());
        list9.add(EnumStatus.OK.name());
        list9.add(EnumStatus.OK.name());

        ArrayList<String> list10 = new ArrayList();
        list10.add(EnumStatus.OK.name());
        list10.add(EnumStatus.WARNING.name());
        list10.add(EnumStatus.WARNING.name());

        expResult.put(Utils.createDate(format, 2021, 0, 15, 0, 0, 0), list1);
        expResult.put(Utils.createDate(format, 2021, 0, 15, 0, 12, 23), list2);
        expResult.put(Utils.createDate(format, 2021, 0, 15, 0, 50, 23), list3);
        expResult.put(Utils.createDate(format, 2021, 0, 15, 1, 5, 10), list4);
        expResult.put(Utils.createDate(format, 2021, 0, 15, 3, 50, 4), list5);
        expResult.put(Utils.createDate(format, 2021, 0, 15, 4, 31, 1), list6);
        expResult.put(Utils.createDate(format, 2021, 0, 15, 5, 20, 15), list7);
        expResult.put(Utils.createDate(format, 2021, 0, 15, 12, 50, 4), list8);
        expResult.put(Utils.createDate(format, 2021, 0, 15, 16, 30, 4), list8);
        expResult.put(Utils.createDate(format, 2021, 0, 15, 21, 3, 5), list10);
        expResult.put(Utils.createDate(format, 2021, 0, 15, 22, 3, 5), list9);
        return expResult;
    }

    private TreeMap<Date, String> createExpResultFinalTimeline() throws ParseException {
        TreeMap<Date, String> expResult = new TreeMap();
        expResult.put(Utils.createDate(format, 2021, 0, 15, 0, 0, 0), EnumStatus.CRITICAL.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 0, 12, 23), EnumStatus.CRITICAL.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 0, 50, 23), EnumStatus.CRITICAL.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 1, 5, 10), EnumStatus.CRITICAL.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 3, 50, 4), EnumStatus.CRITICAL.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 4, 31, 1), EnumStatus.CRITICAL.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 5, 20, 15), EnumStatus.CRITICAL.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 12, 50, 4), EnumStatus.CRITICAL.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 16, 30, 4), EnumStatus.CRITICAL.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 21, 3, 5), EnumStatus.WARNING.name());
        expResult.put(Utils.createDate(format, 2021, 0, 15, 22, 3, 5), EnumStatus.OK.name());

        return expResult;
    }
}
