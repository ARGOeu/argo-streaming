/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package profilesmanager;

import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import static junit.framework.Assert.assertNotNull;
import org.apache.commons.io.IOUtils;
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
public class GroupGroupManagerTest {

    public GroupGroupManagerTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        assertNotNull("Test file missing", GroupGroupManagerTest.class.getResource("/profiles/topgroupgroup.json"));
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
     * Test of insert method, of class GroupGroupManager.
     */
    @Test
    public void testInsert() {
        System.out.println("insert");
        String type = "test";
        String group = "test";
        String subgroup = "test";
        HashMap<String, String> tags = new HashMap<>();
        GroupGroupManager instance = new GroupGroupManager();

        int expResult = 0;
        int result = instance.insert(type, group, subgroup, tags);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getGroupTags method, of class GroupGroupManager.
     */
    @Test
    public void testGetGroupTags() throws IOException {
        System.out.println("getGroupTags");
        String type = "PROJECT";
        String subgroup = "SLA_TEST";
        GroupGroupManager instance = new GroupGroupManager();
        JsonElement jsonElement = loadJson(new File(GroupGroupManagerTest.class.getResource("/profiles/topgroupgroup.json").getFile()));
        instance.loadGroupGroupProfile(jsonElement.getAsJsonArray());

        HashMap<String, String> expResult = new HashMap<>();
        expResult.put("monitored", "1");
        expResult.put("scope", "EGI, SLA");
        HashMap<String, String> result = instance.getGroupTags(type, subgroup);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of count method, of class GroupGroupManager.
     */
    @Test
    public void testCount() throws IOException {
        System.out.println("count");
        GroupGroupManager instance = new GroupGroupManager();
        JsonElement jsonElement = loadJson(new File(GroupGroupManagerTest.class.getResource("/profiles/topgroupgroup.json").getFile()));
        instance.loadGroupGroupProfile(jsonElement.getAsJsonArray());

        int expResult = 31;
        int result = instance.count();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getGroup method, of class GroupGroupManager.
     */
    @Test
    public void testGetGroup() throws IOException {
        System.out.println("getGroup");
        String type = "PROJECT";
        String subgroup = "SLA_TEST";
        GroupGroupManager instance = new GroupGroupManager();
        JsonElement jsonElement = loadJson(new File(GroupGroupManagerTest.class.getResource("/profiles/topgroupgroup.json").getFile()));
        instance.loadGroupGroupProfile(jsonElement.getAsJsonArray());

        String expResult = "EGI";
        String result = instance.getGroup(type, subgroup);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of unfilter method, of class GroupGroupManager.
     */
    @Test
    public void testUnfilter() throws IOException {
        System.out.println("unfilter");
        GroupGroupManager instance = new GroupGroupManager();
        JsonElement jsonElement = loadJson(new File(GroupGroupManagerTest.class.getResource("/profiles/topgroupgroup.json").getFile()));
        instance.loadGroupGroupProfile(jsonElement.getAsJsonArray());

        instance.unfilter();
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of filter method, of class GroupGroupManager.
     */
    @Test
    public void testFilter() throws IOException {
        System.out.println("filter");
        TreeMap<String, String> fTags = new TreeMap<>();
        fTags.put("scope", "test");
        GroupGroupManager instance = new GroupGroupManager();
        JsonElement jsonElement = loadJson(new File(GroupGroupManagerTest.class.getResource("/profiles/topgroupgroup.json").getFile()));
        instance.loadGroupGroupProfile(jsonElement.getAsJsonArray());

        instance.filter(fTags);

        ArrayList<GroupGroupManager.GroupItem> expResult = new ArrayList<>();
        ArrayList<GroupGroupManager.GroupItem> result = instance.getfList();
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of checkSubGroup method, of class GroupGroupManager.
     */
    @Test
    public void testCheckSubGroup() throws IOException {
        System.out.println("checkSubGroup");
        String subgroup = "EGI_WENMR_SLA";
        GroupGroupManager instance = new GroupGroupManager();
        JsonElement jsonElement = loadJson(new File(GroupGroupManagerTest.class.getResource("/profiles/topgroupgroup.json").getFile()));
        instance.loadGroupGroupProfile(jsonElement.getAsJsonArray());

        boolean expResult = true;
        boolean result = instance.checkSubGroup(subgroup);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of loadAvro method, of class GroupGroupManager.
     */
//    @Test
//    public void testLoadAvro() throws Exception {
//        System.out.println("loadAvro");
//        File avroFile = null;
//        GroupGroupManager instance = new GroupGroupManager();
//        instance.loadAvro(avroFile);
//        // TODO review the generated test code and remove the default call to fail.
//        //fail("The test case is a prototype.");
//    }
    /**
     * Test of loadFromList method, of class GroupGroupManager.
     */
    @Test
    public void testLoadFromList() throws IOException {
        System.out.println("loadFromList");
        GroupGroupManager instance = new GroupGroupManager();
        JsonElement jsonElement = loadJson(new File(GroupGroupManagerTest.class.getResource("/profiles/topgroupgroup.json").getFile()));

        GroupGroup[] groupGroup = instance.readJson(jsonElement.getAsJsonArray());
        List<GroupGroup> ggp = Arrays.asList(groupGroup);
        instance.loadFromList(ggp);

    }

    /**
     * Test of loadGroupGroupProfile method, of class GroupGroupManager.
     */
    @Test
    public void testLoadGroupGroupProfile() throws Exception {
        System.out.println("loadGroupGroupProfile");
        System.out.println("loadGroupEndpointProfile");
        GroupGroupManager instance = new GroupGroupManager();
        JsonElement jsonElement = loadJson(new File(GroupGroupManagerTest.class.getResource("/profiles/topgroupgroup.json").getFile()));
    
        instance.loadGroupGroupProfile(jsonElement.getAsJsonArray());
   
        // TODO review the generated test code and remove the default call to fail.
      //  fail("The test case is a prototype.");
    }

    /**
     * Test of readJson method, of class GroupGroupManager.
     */
//    @Test
//    public void testReadJson() {
//        System.out.println("readJson");
//        JsonArray jElement = null;
//        GroupGroupManager instance = new GroupGroupManager();
//        GroupGroup[] expResult = null;
//        GroupGroup[] result = instance.readJson(jElement);
//        assertArrayEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }

    private JsonElement loadJson(File jsonFile) throws IOException {

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(jsonFile));

            JsonParser json_parser = new JsonParser();
            JsonElement j_element = json_parser.parse(br);
            JsonObject jRoot = j_element.getAsJsonObject();
            JsonArray jData = jRoot.get("data").getAsJsonArray();
//            JsonElement jItem = jData.get(0);

            return jData;
        } catch (FileNotFoundException ex) {
            //  LOG.error("Could not open file:" + jsonFile.getName());
            throw ex;

        } catch (JsonParseException ex) {
            //   LOG.error("File is not valid json:" + jsonFile.getName());
            throw ex;
        } finally {
            // Close quietly without exceptions the buffered reader
            IOUtils.closeQuietly(br);
        }

    }

}
