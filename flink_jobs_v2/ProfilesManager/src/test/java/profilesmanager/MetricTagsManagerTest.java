package profilesmanager;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import static junit.framework.Assert.assertNotNull;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *MetricTagsManagerTest is a unit test to test MetricTagsManager
 */
public class MetricTagsManagerTest {

    public MetricTagsManagerTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        assertNotNull("Test file missing", MetricProfileManagerTest.class.getResource("/profiles/metric_tags.json"));

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
     * Test of clear method, of class MetricTagsManager.
     */
    @Test
    public void testClear() throws IOException {
        System.out.println("clear");
        MetricTagsManager instance = new MetricTagsManager();

        File jsonFile = new File(MetricTagsManagerTest.class.getResource("/profiles/metric_tags.json").getFile());
        instance.loadJson(jsonFile);
        instance.clear();
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of loadJson method, of class MetricTagsManager.
     */
    @Test
    public void testLoadJson() throws Exception {
        System.out.println("loadJson");
        File jsonFile = new File(MetricTagsManagerTest.class.getResource("/profiles/metric_tags.json").getFile());
        MetricTagsManager instance = new MetricTagsManager();

        instance.loadJson(jsonFile);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of insert method, of class MetricTagsManager.
     */
    @Test
    public void testInsert() throws Exception {
        System.out.println("insertMetricTags");
        MetricTagsManager instance = new MetricTagsManager();
        String metric = "metric1";
        ArrayList<String> tags = new ArrayList<>();
        String tag1 = "tag1";
        String tag2 = "tags2";
        instance.insert(metric, tag1);
        instance.insert(metric, tag2);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getTags method, of class MetricTagsManager.
     */
    @Test
    public void testGetTags() throws Exception {
        System.out.println("getMetricTags");

        MetricTagsManager instance = new MetricTagsManager();
        String metric1 = "ch.cern.HTCondorCE-JobSubmit";

        String metric2 = "ch.cern.HTCondorCE-JobState";
        ArrayList<String> tags1 = new ArrayList<>();
        String tag1 = "none";
        String tag2 = "deprecated";
        String tag3 = "internal";
        tags1.add(tag1);
        ArrayList<String> tags2 = new ArrayList<>();
        tags2.add(tag3);
        tags2.add(tag2);

        File jsonFile = new File(MetricTagsManagerTest.class.getResource("/profiles/metric_tags2.json").getFile());
        instance.loadJson(jsonFile);
        ArrayList<String> resMetricTags1 = instance.getTags(metric1);

        ArrayList<String> resMetricTags2 = instance.getTags(metric2);
        assertEquals(tags2, resMetricTags2);
        assertEquals(tags1, resMetricTags1);

        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

}
