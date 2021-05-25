package argo.commons.profiles;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import argo.commons.timelines.TimelineMergerTest;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
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
public class TopologyGroupParserTest {

    public TopologyGroupParserTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        assertNotNull("Test file missing", TopologyGroupParserTest.class.getResource("/profiles/topgroup.json"));
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
     * Test of containsGroup method, of class TopologyGroupParser.
     */
    @Test
    public void testContainsGroup() throws IOException, FileNotFoundException, ParseException {
        System.out.println("containsGroup");
        String group = "EGI";
        JSONObject topGroupJSONObject = readJsonFromFile(TopologyGroupParserTest.class.getResource("/profiles/topgroup.json").getFile());
        TopologyGroupParser instance = new TopologyGroupParser(topGroupJSONObject);

        boolean expResult = false;
        boolean result = instance.containsGroup(group);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of getTopologyGroupsPerType method, of class TopologyGroupParser.
     */
    @Test
    public void testGetTopologyGroupsPerType() throws IOException, ParseException {
        System.out.println("getTopologyGroupsPerType");

         JSONObject topGroupJSONObject = readJsonFromFile(TopologyGroupParserTest.class.getResource("/profiles/topgroup.json").getFile());
        TopologyGroupParser instance = new TopologyGroupParser(topGroupJSONObject);

        HashMap<String, ArrayList<TopologyGroupParser.TopologyGroup>> expResult = createTopologyPerType();
        HashMap<String, ArrayList<TopologyGroupParser.TopologyGroup>> result = instance.getTopologyGroupsPerType();
         assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of setTopologyGroupsPerType method, of class TopologyGroupParser.
     */
    @Test
    public void testSetTopologyGroupsPerType() throws IOException, ParseException {
        System.out.println("setTopologyGroupsPerType");
        HashMap<String, ArrayList<TopologyGroupParser.TopologyGroup>> topologyGroupsPerType = createTopologyPerType();

        JSONObject topGroupJSONObject = readJsonFromFile(TopologyGroupParserTest.class.getResource("/profiles/topgroup.json").getFile());
        TopologyGroupParser instance = new TopologyGroupParser(topGroupJSONObject);

        instance.setTopologyGroupsPerType(topologyGroupsPerType);
        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
    }

    /**
     * Test of getTopologyGroups method, of class TopologyGroupParser.
     */
    @Test
    public void testGetTopologyGroups() throws IOException, ParseException {
        System.out.println("getTopologyGroups");
 JSONObject topGroupJSONObject = readJsonFromFile(TopologyGroupParserTest.class.getResource("/profiles/topgroup.json").getFile());
        TopologyGroupParser instance = new TopologyGroupParser(topGroupJSONObject);

        
        ArrayList<String> expResult = createGroups();
        ArrayList<String> result = instance.getTopologyGroups();
         assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //   fail("The test case is a prototype.");
    }

    /**
     * Test of setTopologyGroups method, of class TopologyGroupParser.
     */
    @Test
    public void testSetTopologyGroups() throws IOException, ParseException {
        System.out.println("setTopologyGroups");
        ArrayList<String> topologyGroups = createGroups();

        JSONObject topGroupJSONObject = readJsonFromFile(TopologyGroupParserTest.class.getResource("/profiles/topgroup.json").getFile());
        TopologyGroupParser instance = new TopologyGroupParser(topGroupJSONObject);

        
        instance.setTopologyGroups(topologyGroups);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    private HashMap<String, ArrayList<TopologyGroupParser.TopologyGroup>> createTopologyPerType() {
        TopologyGroupParser groupParser = new TopologyGroupParser();

        TopologyGroupParser.TopologyGroup topGroup1 = groupParser.new TopologyGroup("EGI", "PROJECT", "SLA_TEST", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup2 = groupParser.new TopologyGroup("EGI", "PROJECT", "SLA_TEST_B", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup3 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_D4SCIENCE_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup4 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_WENMR_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup5 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_OBSEA_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup6 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_NBISBILS_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup7 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_MSO4SC_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup8 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_BIOISI_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup9 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_TERRADUE_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup10 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_GEODAB_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup11 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_OPENUNIVERSE_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup12 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_CLARIN_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup13 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_DEIMOS_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup14 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_EMSOERIC_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup15 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_TrainingInfrastructure_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup16 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_Notebooks_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup17 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_AoD_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup18 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_EXTraS_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup19 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_Fusion_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup20 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_LSGC_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup21 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_MRILab_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup22 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_Peachnote_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup23 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_OPENBIOMAP_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup24 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_IIASA_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup25 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_EMPHASIS_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup26 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_EOSCSYNERGY_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup27 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_ENVRIFAIR_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup28 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_VESPA_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup29 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_ECRIN_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup30 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_STARS4ALL_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);
        TopologyGroupParser.TopologyGroup topGroup31 = groupParser.new TopologyGroup("EGI", "PROJECT", "EGI_GOSAFE_SLA", groupParser.new Tags("EGI, SLA", "1", null), null);

        ArrayList<TopologyGroupParser.TopologyGroup> listGroups = new ArrayList<>();
        listGroups.add(topGroup1);
        listGroups.add(topGroup2);
        listGroups.add(topGroup3);
        listGroups.add(topGroup4);
        listGroups.add(topGroup5);
        listGroups.add(topGroup6);
        listGroups.add(topGroup7);
        listGroups.add(topGroup8);
        listGroups.add(topGroup9);
        listGroups.add(topGroup10);
        listGroups.add(topGroup11);
        listGroups.add(topGroup12);
        listGroups.add(topGroup13);
        listGroups.add(topGroup14);
        listGroups.add(topGroup15);
        listGroups.add(topGroup16);
        listGroups.add(topGroup17);
        listGroups.add(topGroup18);
        listGroups.add(topGroup19);
        listGroups.add(topGroup20);
        listGroups.add(topGroup21);
        listGroups.add(topGroup22);
        listGroups.add(topGroup23);
        listGroups.add(topGroup24);
        listGroups.add(topGroup25);
        listGroups.add(topGroup26);
        listGroups.add(topGroup27);
        listGroups.add(topGroup28);
        listGroups.add(topGroup29);
        listGroups.add(topGroup30);
        listGroups.add(topGroup31);

        HashMap<String, ArrayList<TopologyGroupParser.TopologyGroup>> map = new HashMap<>();
        map.put("PROJECT", listGroups);
        return map;
    }

    private ArrayList<String> createGroups() {

        ArrayList<String> list = new ArrayList<>();

        list.add("SLA_TEST");
        list.add("SLA_TEST_B");
        list.add("EGI_D4SCIENCE_SLA");
        list.add("EGI_WENMR_SLA");
        list.add("EGI_OBSEA_SLA");
        list.add("EGI_NBISBILS_SLA");
        list.add("EGI_MSO4SC_SLA");
        list.add("EGI_BIOISI_SLA");
        list.add("EGI_TERRADUE_SLA");
        list.add("EGI_GEODAB_SLA");
        list.add("EGI_OPENUNIVERSE_SLA");
        list.add("EGI_CLARIN_SLA");
        list.add("EGI_DEIMOS_SLA");
        list.add("EGI_EMSOERIC_SLA");
        list.add("EGI_TrainingInfrastructure_SLA");
        list.add("EGI_Notebooks_SLA");
        list.add("EGI_AoD_SLA");
        list.add("EGI_EXTraS_SLA");
        list.add("EGI_Fusion_SLA");
        list.add("EGI_LSGC_SLA");
        list.add("EGI_MRILab_SLA");
        list.add("EGI_Peachnote_SLA");
        list.add("EGI_OPENBIOMAP_SLA");
        list.add("EGI_IIASA_SLA");
        list.add("EGI_EMPHASIS_SLA");
        list.add("EGI_EOSCSYNERGY_SLA");
        list.add("EGI_ENVRIFAIR_SLA");
        list.add("EGI_VESPA_SLA");
        list.add("EGI_ECRIN_SLA");
        list.add("EGI_STARS4ALL_SLA");
        list.add("EGI_GOSAFE_SLA");
        return list;
    }

    private JSONObject readJsonFromFile(String path) throws FileNotFoundException, IOException, org.json.simple.parser.ParseException {
        JSONParser parser = new JSONParser();
              Object obj = parser.parse(new FileReader(path));

        JSONObject jsonObject = (JSONObject) obj;

        return jsonObject;
    }

}
