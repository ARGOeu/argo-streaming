/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package profilesmanager;

import argo.avro.GroupEndpoint;
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


public class EndpointGroupManagerTest {

    public EndpointGroupManagerTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        assertNotNull("Test file missing", EndpointGroupManagerTest.class.getResource("/profiles/topgroupendpoint.json"));
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
     * Test of insert method, of class EndpointGroupManager.
     */
    @Test
    public void testInsert() throws IOException {
        System.out.println("insert");
        String type = "test";
        String group = "test";
        String service = "test";
        String hostname = "test";
        HashMap<String, String> tags = new HashMap();
        tags.put("scope", "test");
        EndpointGroupManager instance = new EndpointGroupManager();
        int expResult = 0;
        int result = instance.insert(type, group, service, hostname, tags);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of checkEndpoint method, of class EndpointGroupManager.
     */
    @Test
    public void testCheckEndpoint() throws IOException {
        System.out.println("checkEndpoint");
        String hostname = "bdii.marie.hellasgrid.gr";
        String service = "Top-BDII";
        EndpointGroupManager instance = new EndpointGroupManager();
        JsonElement jsonElement = loadJson(new File(EndpointGroupManagerTest.class.getResource("/profiles/topgroupendpoint.json").getFile()));
        instance.loadGroupEndpointProfile(jsonElement.getAsJsonArray());

        boolean expResult = true;
        boolean result = instance.checkEndpoint(hostname, service);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of getGroup method, of class EndpointGroupManager.
     */
    @Test
    public void testGetGroup() throws IOException {
        System.out.println("getGroup");
        String type = "SERVICEGROUPS";
        String hostname = "bdii.marie.hellasgrid.gr";
        String service = "Top-BDII";

        EndpointGroupManager instance = new EndpointGroupManager();
        JsonElement jsonElement = loadJson(new File(EndpointGroupManagerTest.class.getResource("/profiles/topgroupendpoint.json").getFile()));
        instance.loadGroupEndpointProfile(jsonElement.getAsJsonArray());

        ArrayList<String> expResult = new ArrayList<>();
        expResult.add("SLA_TEST");
        ArrayList<String> result = instance.getGroup(type, hostname, service);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //   fail("The test case is a prototype.");
    }

    /**
     * Test of getInfo method, of class EndpointGroupManager.
     */
    @Test
    public void testGetInfo() throws IOException {
        System.out.println("getInfo");
        String group = "SLA_TEST";
        String type = "SERVICEGROUPS";
        String hostname = "bdii.marie.hellasgrid.gr";
        String service = "Top-BDII";

        EndpointGroupManager instance = new EndpointGroupManager();
        JsonElement jsonElement = loadJson(new File(EndpointGroupManagerTest.class.getResource("/profiles/topgroupendpoint.json").getFile()));
        instance.loadGroupEndpointProfile(jsonElement.getAsJsonArray());

        String expResult = "";
        String result = instance.getInfo(group, type, hostname, service);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getGroupTags method, of class EndpointGroupManager.
     */
    @Test
    public void testGetGroupTags() throws IOException {
        System.out.println("getGroupTags");
        String group = "SLA_TEST";
        String type = "SERVICEGROUPS";
        String hostname = "bdii.marie.hellasgrid.gr";
        String service = "Top-BDII";

        EndpointGroupManager instance = new EndpointGroupManager();

        JsonElement jsonElement = loadJson(new File(EndpointGroupManagerTest.class.getResource("/profiles/topgroupendpoint.json").getFile()));
        instance.loadGroupEndpointProfile(jsonElement.getAsJsonArray());

        HashMap<String, String> expResult = new HashMap();
        expResult.put("monitored", "1");
        expResult.put("production", "1");
        expResult.put("scope", "EGI");
        HashMap<String, String> result = instance.getGroupTags(group, type, hostname, service);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of count method, of class EndpointGroupManager.
     */
    @Test
    public void testCount() throws IOException {
        System.out.println("count");
        EndpointGroupManager instance = new EndpointGroupManager();
        JsonElement jsonElement = loadJson(new File(EndpointGroupManagerTest.class.getResource("/profiles/topgroupendpoint.json").getFile()));
        instance.loadGroupEndpointProfile(jsonElement.getAsJsonArray());

        int expResult = 246;
        int result = instance.count();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of unfilter method, of class EndpointGroupManager.
     */
    @Test
    public void testUnfilter() throws IOException {
        System.out.println("unfilter");
        EndpointGroupManager instance = new EndpointGroupManager();
        JsonElement jsonElement = loadJson(new File(EndpointGroupManagerTest.class.getResource("/profiles/topgroupendpoint.json").getFile()));
        instance.loadGroupEndpointProfile(jsonElement.getAsJsonArray());

        instance.unfilter();
        // TODO review the generated test code and remove the default call to fail.
        //   fail("The test case is a prototype.");
    }

    /**
     * Test of filter method, of class EndpointGroupManager.
     */
    @Test
    public void testFilter() throws IOException {
        System.out.println("filter");
        TreeMap<String, String> fTags = new TreeMap();
        fTags.put("monitored", "1");
        fTags.put("production", "1");
        fTags.put("scope", "EGI, wlcg, tier2, alice, FedCloud");
        EndpointGroupManager instance = new EndpointGroupManager();
        JsonElement jsonElement = loadJson(new File(EndpointGroupManagerTest.class.getResource("/profiles/topgroupendpoint.json").getFile()));
        instance.loadGroupEndpointProfile(jsonElement.getAsJsonArray());
        HashMap<String, String> tags1 = new HashMap();
        tags1.put("monitored", "1");
        tags1.put("production", "1");
        tags1.put("scope", "EGI, wlcg, tier2, alice, FedCloud");
        EndpointGroupManager.EndpointItem groupEndpoint1 = instance.new EndpointItem("SERVICEGROUPS", "EGI_WENMR_SLA", "org.openstack.nova", "openstack.bitp.kiev.ua", tags1);
        EndpointGroupManager.EndpointItem groupEndpoint2 = instance.new EndpointItem("SERVICEGROUPS", "EGI_AoD_SLA", "org.openstack.nova", "openstack.bitp.kiev.ua", tags1);
        EndpointGroupManager.EndpointItem groupEndpoint3 = instance.new EndpointItem("SERVICEGROUPS", "EGI_AoD_SLA", "eu.egi.cloud.vm-metadata.vmcatcher", "openstack.bitp.kiev.ua", tags1);

        //ArrayList<EndpointGroupManager.EndpointItem> expResult=instance.getList();
        ArrayList<EndpointGroupManager.EndpointItem> expResult=new ArrayList<>();
        expResult.add(groupEndpoint1);
        expResult.add(groupEndpoint2);
        expResult.add(groupEndpoint3);
        
//        Iterator<EndpointGroupManager.EndpointItem> iter=expResult.iterator();
//        while(iter.hasNext()){
//            if(iter.next().equals(groupEndpoint1)|| iter.next().equals(groupEndpoint2)|| iter.next().equals(groupEndpoint3)){
//                iter.remove();
//            }
//        }
        
        instance.filter(fTags);

        ArrayList< EndpointGroupManager.EndpointItem> result = instance.getfList();

        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

//    /**
//     * Test of loadAvro method, of class EndpointGroupManager.
//     */
//    @Test
//    public void testLoadAvro() throws Exception {
//        System.out.println("loadAvro");
//        File avroFile = null;
//        EndpointGroupManager instance = new EndpointGroupManager();
//        instance.loadAvro(avroFile);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }

    /**
     * Test of getList method, of class EndpointGroupManager.
     */
//    @Test
//    public void testGetList() {
//        System.out.println("getList");
//        EndpointGroupManager instance = new EndpointGroupManager();
//        ArrayList expResult = null;
//        ArrayList result = instance.getList();
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }

    /**
     * Test of loadFromList method, of class EndpointGroupManager.
     */
    @Test
    public void testLoadFromList() throws IOException {
        System.out.println("loadFromList");
         EndpointGroupManager instance = new EndpointGroupManager();
        JsonElement jsonElement = loadJson(new File(EndpointGroupManagerTest.class.getResource("/profiles/topgroupendpoint.json").getFile()));
     
      GroupEndpoint[] groupEndpoint=instance.readJson(jsonElement.getAsJsonArray());
        List<GroupEndpoint> egp = Arrays.asList(groupEndpoint);
        instance.loadFromList(egp);
     
   //    fail("The test case is a prototype.");
    
    }

    /**
     * Test of loadGroupEndpointProfile method, of class EndpointGroupManager.
     */
    @Test
    public void testLoadGroupEndpointProfile() throws Exception {
        System.out.println("loadGroupEndpointProfile");
        EndpointGroupManager instance = new EndpointGroupManager();
        JsonElement jsonElement = loadJson(new File(EndpointGroupManagerTest.class.getResource("/profiles/topgroupendpoint.json").getFile()));
    
        instance.loadGroupEndpointProfile(jsonElement.getAsJsonArray());
        // TODO review the generated test code and remove the default call to fail.
       // fail("The test case is a prototype.");
    }

//    /**
//     * Test of readJson method, of class EndpointGroupManager.
//     */
//    @Test
//    public void testReadJson() {
//        System.out.println("readJson");
//        JsonArray jElement = null;
//        EndpointGroupManager instance = new EndpointGroupManager();
//        GroupEndpoint[] expResult = null;
//        GroupEndpoint[] result = instance.readJson(jElement);
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
