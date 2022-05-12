package timelines;

import java.net.URL;
import java.text.ParseException;
import java.util.TreeMap;
import static junit.framework.Assert.assertNotNull;
import org.joda.time.DateTime;
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
 * A unit test to test TImelineIntegrator class
 */
public class TimelineIntegratorTest {

    public TimelineIntegratorTest() {
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
     * Test of countStatusAppearances method, of class Timeline.
     */
    @Test
    public void testCountStatusAppearances() throws ParseException {
        System.out.println("countStatusAppearances");
        int status = 0;
        Timeline instance = new Timeline();
        instance.insertDateTimeStamps(createTimestampList(), true);
        TimelineIntegrator tIntegrator = new TimelineIntegrator();
        int expResult = 1;

        int result[] = tIntegrator.countStatusAppearances(instance.getSamples(), status);
        assertEquals(expResult, result[0]);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    @Test
    public void testCalcAR() throws ParseException {
        System.out.println("testCalcAR");

        TreeMap<String, Integer> samples = createArTimestampList();
        Timeline timeline = new Timeline();
        timeline.insertStringTimeStamps(samples, true);
        DateTime dt = Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 0, 0, 0);
        TimelineIntegrator tIntegrator = new TimelineIntegrator();
        tIntegrator.calcAR(timeline.getSamples(), dt, 0, 1, 2, 5, 3);
        double avail = tIntegrator.getAvailability();
        double rel = tIntegrator.getReliability();
        double up = tIntegrator.getUp_f();
        double unknown = tIntegrator.getUnknown_f();
        double down = tIntegrator.getDown_f();

        assertEquals(25, avail, 0.10f);
        assertEquals(25, rel, 0.10f);
        assertEquals(0.208, up, 0.01f);
        assertEquals(0.16, unknown, 0.01f);
        assertEquals(0, down, 0.10f);

        samples = createNanTimestampList();
        timeline = new Timeline();
        timeline.insertStringTimeStamps(samples, true);
        tIntegrator = new TimelineIntegrator();
        tIntegrator.calcAR(timeline.getSamples(), dt, 0, 1, 2, 5, 3);
        avail = tIntegrator.getAvailability();
        rel = tIntegrator.getReliability();
        up = tIntegrator.getUp_f();
        unknown = tIntegrator.getUnknown_f();
        down = tIntegrator.getDown_f();

        assertEquals(-1, avail, 0);
        assertEquals(-1, rel, 0);
        assertEquals(0, up, 0);
        assertEquals(1, unknown, 0);
        assertEquals(0, down, 0);

        samples = createArDownTimestampList();
        timeline = new Timeline();
        timeline.insertStringTimeStamps(samples, true);
        tIntegrator = new TimelineIntegrator();

        tIntegrator.calcAR(timeline.getSamples(), dt, 0, 1, 2, 5, 3);
        avail = tIntegrator.getAvailability();
        rel = tIntegrator.getReliability();
        up = tIntegrator.getUp_f();
        unknown = tIntegrator.getUnknown_f();
        down = tIntegrator.getDown_f();

        assertEquals(27.8, avail, 0.10f);
        assertEquals(35.7, rel, 0.10f);
        assertEquals(0.208, up, 0.01f);
        assertEquals(0.25, unknown, 0.01f);
        assertEquals(0.166, down, 0.10f);

    }

    private TreeMap<String, Integer> createArTimestampList() throws ParseException {
        TreeMap<String, Integer> map = new TreeMap<>();

        DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 0, 0, 0).toString(dtf), 0);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 3, 0, 0).toString(dtf), 4);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 18, 0, 0).toString(dtf), 2);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 22, 0, 0).toString(dtf), 0);
        return map;
//
    }

    private TreeMap<String, Integer> createNanTimestampList() throws ParseException {
        TreeMap<String, Integer> map = new TreeMap<>();

        DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 0, 0, 0).toString(dtf), 3);
        return map;
//
    }

    private TreeMap<String, Integer> createArDownTimestampList() throws ParseException {
        TreeMap<String, Integer> map = new TreeMap<>();

        DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 0, 0, 0).toString(dtf), 0);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 3, 0, 0).toString(dtf), 4);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 12, 0, 0).toString(dtf), 2);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 18, 0, 0).toString(dtf), 5);
        map.put(Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", 2021, 0, 15, 22, 0, 0).toString(dtf), 0);
        return map;
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
    }
}