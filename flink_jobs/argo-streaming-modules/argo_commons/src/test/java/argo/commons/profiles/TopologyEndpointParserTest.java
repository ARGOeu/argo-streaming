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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author cthermolia
 */
public class TopologyEndpointParserTest {

    public TopologyEndpointParserTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        assertNotNull("Test file missing", TopologyEndpointParserTest.class.getResource("/profiles/topendp.json"));
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
     * Test of getTopology method, of class TopologyEndpointParser.
     */
    @Test
    public void testGetTopology() throws Exception {
        System.out.println("getTopology");
        String type = "SITES";
        JSONObject topEndpJSONObject = readJsonFromFile(TopologyEndpointParserTest.class.getResource("/profiles/topendp.json").getFile());
        TopologyEndpointParser instance = new TopologyEndpointParser(topEndpJSONObject);

        HashMap<String, String> expResult = createEndpoints2();
        HashMap<String, String> result = instance.getTopology(type);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of retrieveGroup method, of class TopologyEndpointParser.
     */
    @Test
    public void testRetrieveGroup() throws IOException, ParseException {
        System.out.println("retrieveGroup");
        String type = "SITES";
        String serviceEndpoint = "bdii.goegrid.gwdg.de" + "-" + "Site-BDII";
        JSONObject topEndpJSONObject = readJsonFromFile(TopologyEndpointParserTest.class.getResource("/profiles/topendp.json").getFile());
        TopologyEndpointParser instance = new TopologyEndpointParser(topEndpJSONObject);

        String expResult = "GoeGrid";
        String result = instance.retrieveGroup(type, serviceEndpoint);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getTopologyEndPointsPerType method, of class
     * TopologyEndpointParser.
     */
    @Test
    public void testGetTopologyEndPointsPerType() throws IOException, ParseException {
        System.out.println("getTopologyEndPointsPerType");
        JSONObject topEndpJSONObject = readJsonFromFile(TopologyEndpointParserTest.class.getResource("/profiles/topendp.json").getFile());
        TopologyEndpointParser instance = new TopologyEndpointParser(topEndpJSONObject);

        HashMap<String, ArrayList<TopologyEndpointParser.EndpointGroup>> expResult = new HashMap<>();
        expResult.put("SERVICEGROUPS", createTopologyEndpoint1());
        expResult.put("SITES", createTopologyEndpoint2());
        HashMap<String, ArrayList<TopologyEndpointParser.EndpointGroup>> result = instance.getTopologyEndPointsPerType();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //   fail("The test case is a prototype.");
    }

    /**
     * Test of setTopologyEndPointsPerType method, of class
     * TopologyEndpointParser.
     */
    @Test
    public void testSetTopologyEndPointsPerType() throws IOException, ParseException {
        System.out.println("setTopologyEndPointsPerType");
        HashMap<String, ArrayList<TopologyEndpointParser.EndpointGroup>> topologyEndPointsPerType = new HashMap<>();
        topologyEndPointsPerType.put("SERVICEGROUPS", createTopologyEndpoint1());
        topologyEndPointsPerType.put("SITES", createTopologyEndpoint2());

        JSONObject topEndpJSONObject = readJsonFromFile(TopologyEndpointParserTest.class.getResource("/profiles/topendp.json").getFile());
        TopologyEndpointParser instance = new TopologyEndpointParser(topEndpJSONObject);

        instance.setTopologyEndPointsPerType(topologyEndPointsPerType);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getTopologyEndpoint method, of class TopologyEndpointParser.
     */
    @Test
    public void testGetTopologyEndpoint() throws ParseException, IOException {
        System.out.println("getTopologyEndpoint");

        JSONObject topEndpJSONObject = readJsonFromFile(TopologyEndpointParserTest.class.getResource("/profiles/topendp.json").getFile());
        TopologyEndpointParser instance = new TopologyEndpointParser(topEndpJSONObject);

        HashMap<String, HashMap<String, String>> expResult = new HashMap<>();
        expResult.put("SERVICEGROUPS", createEndpoints1());
        expResult.put("SITES", createEndpoints2());

        HashMap<String, HashMap<String, String>> result = instance.getTopologyEndpoint();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    /**
     * Test of setTopologyEndpoint method, of class TopologyEndpointParser.
     */
    @Test
    public void testSetTopologyEndpoint() throws IOException, ParseException {
        System.out.println("setTopologyEndpoint");
        HashMap<String, HashMap<String, String>> topologyEndpoint = new HashMap<>();
        topologyEndpoint.put("SERVICEGROUPS", createEndpoints1());
        topologyEndpoint.put("SITES", createEndpoints2());

        JSONObject topEndpJSONObject = readJsonFromFile(TopologyEndpointParserTest.class.getResource("/profiles/topendp.json").getFile());
        TopologyEndpointParser instance = new TopologyEndpointParser(topEndpJSONObject);

        instance.setTopologyEndpoint(topologyEndpoint);
        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
    }

    private HashMap<String, String> createEndpoints1() {

        HashMap<String, String> map = new HashMap<>();

        ArrayList<TopologyEndpointParser.EndpointGroup> list = createTopologyEndpoint1();

        for (TopologyEndpointParser.EndpointGroup endpg : list) {

            map.put(endpg.getHostname() + "-" + endpg.getService(), endpg.getGroup());

        }
        return map;
    }

    private HashMap<String, String> createEndpoints2() {

        HashMap<String, String> map = new HashMap<>();

        ArrayList<TopologyEndpointParser.EndpointGroup> list = createTopologyEndpoint2();

        for (TopologyEndpointParser.EndpointGroup endpg : list) {

            map.put(endpg.getHostname() + "-" + endpg.getService(), endpg.getGroup());

        }
        return map;
    }

    private ArrayList<TopologyEndpointParser.EndpointGroup> createTopologyEndpoint1() {
        ArrayList<TopologyEndpointParser.EndpointGroup> list = new ArrayList<>();
        TopologyEndpointParser parser = new TopologyEndpointParser();
        String group = "SLA_TEST";
        String type = "SERVICEGROUPS";
        String service = "Top-BDII";
        String hostname = "bdii.marie.hellasgrid.gr";
        String monitored = "1";
        String production = "1";
        String scope = "EGI";

        TopologyEndpointParser.EndpointGroup endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "SLA_TEST";
        type = "SERVICEGROUPS";
        service = "Top-BDII";
        hostname = "bdii01.athena.hellasgrid.gr";

        monitored = "1";
        production = "1";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "SLA_TEST";
        type = "SERVICEGROUPS";
        service = "Top-BDII";
        hostname = "bdii02.athena.hellasgrid.gr";
        monitored = "1";
        production = "1";
        scope = "EGI";

        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "SLA_TEST";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "cream-ce01.marie.hellasgrid.gr";
        monitored = "1";
        production = "1";
        scope = "EGI, cms";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "SLA_TEST";
        type = "SERVICEGROUPS";
        service = "SRM";
        hostname = "se01.marie.hellasgrid.gr";
        monitored = "1";
        production = "1";
        scope = "EGI, cms";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "SLA_TEST_B";
        type = "SERVICEGROUPS";
        service = "UI";
        hostname = "ui01.isabella.grnet.gr";
        monitored = "1";
        production = "1";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "SLA_TEST_B";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "snf-189278.vm.okeanos.grnet.gr";
        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "SLA_TEST_B";
        type = "SERVICEGROUPS";
        service = "APEL";
        hostname = "snf-189278.vm.okeanos.grnet.gr";
        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "SLA_TEST_B";
        type = "SERVICEGROUPS";
        service = "Site-BDII";
        hostname = "snf-189279.vm.okeanos.grnet.gr";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "SLA_TEST_B";
        type = "SERVICEGROUPS";
        service = "gLite-APEL";
        hostname = "snf-189279.vm.okeanos.grnet.gr";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "SLA_TEST_B";
        type = "SERVICEGROUPS";
        service = "emi.ARGUS";
        hostname = "snf-451878.vm.okeanos.grnet.gr";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "SLA_TEST_B";
        type = "SERVICEGROUPS";
        service = "eu.egi.MPI";
        hostname = "snf-189278.vm.okeanos.grnet.gr";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_D4SCIENCE_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "api.cloud.ifca.es";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, cms, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_D4SCIENCE_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "fedcloud-osservices.egi.cesga.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "Site-BDII";
        hostname = "sbdii01.ncg.ingrid.pt";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, atlas, cms";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "Site-BDII";
        hostname = "prod-bdii-02.pd.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "Site-BDII";
        hostname = "sitebdii.grid.sara.nl";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier1, alice, atlas, lhcb";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "Site-BDII";
        hostname = "siteinfo03.nikhef.nl";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier1, alice, atlas, lhcb";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "ce06.ncg.ingrid.pt";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "prod-ce-01.pd.infn.it";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "creamce3.gina.sara.nl";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier1, alice, atlas, lhcb";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "creamce.gina.sara.nl";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier1, alice, atlas, lhcb";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "creamce2.gina.sara.nl";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier1, alice, atlas, lhcb";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "klomp.nikhef.nl";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier1, alice, atlas, lhcb";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "gazon.nikhef.nl";

        monitored = "1";
        production = "1";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "juk.nikhef.nl";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier1, alice, atlas, lhcb";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "stremsel.nikhef.nl";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier1, alice, atlas, lhcb";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "SRM";
        hostname = "storm.ifca.es";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, cms";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "SRM";
        hostname = "srm01.ifca.es";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, cms";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "SRM";
        hostname = "srm01.ncg.ingrid.pt";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, atlas, cms";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "SRM";
        hostname = "prod-se-01.pd.infn.it";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "SRM";
        hostname = "srm.grid.sara.nl";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier1, alice, atlas, lhcb";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "carach5.ics.muni.cz";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "Site-BDII";
        hostname = "ce01.grid.nchc.org.tw";

        monitored = "1";
        production = "1";
        scope = "EGI, cms";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "SRM";
        hostname = "se01.grid.nchc.org.tw";

        monitored = "1";
        production = "1";
        scope = "EGI, cms";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "api.cloud.ifca.es";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, cms, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "openstack.bitp.kiev.ua";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, alice, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "webdav";
        hostname = "gftp01.ncg.ingrid.pt";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, atlas, cms, EOSC-hub";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "ARC-CE";
        hostname = "ce02.grid.nchc.org.tw";

        monitored = "0";
        production = "0";
        scope = "EGI, cms";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "egi-cloud.pd.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "fedcloud-osservices.egi.cesga.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "webdav";
        hostname = "se01.grid.nchc.org.tw";

        monitored = "1";
        production = "0";
        scope = "EGI, cms";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_WENMR_SLA";
        type = "SERVICEGROUPS";
        service = "globus-GRIDFTP";
        hostname = "se01.grid.nchc.org.tw";

        monitored = "1";
        production = "0";
        scope = "EGI, cms";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_OBSEA_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "stack-server.ct.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_NBISBILS_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "sbgcloud.in2p3.fr";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, alice, cms, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_NBISBILS_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "cloud.recas.ba.infn.it";

        monitored = "0";
        production = "0";
        scope = "EGI, wlcg, lhcb, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_NBISBILS_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "cloud.recas.ba.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, lhcb, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_MSO4SC_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "fedcloud-services.egi.cesga.es";

        monitored = "1";
        production = "0";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_MSO4SC_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "fedcloud-osservices.egi.cesga.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_BIOISI_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "sbgcloud.in2p3.fr";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, alice, cms, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_BIOISI_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "stratus.ncg.ingrid.pt";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud, EOSC-hub";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_TERRADUE_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "cloud.recas.ba.infn.it";

        monitored = "0";
        production = "0";
        scope = "EGI, wlcg, lhcb, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_TERRADUE_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "cloud.recas.ba.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, lhcb, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_TERRADUE_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "fedcloud-services.egi.cesga.es";

        monitored = "1";
        production = "0";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_TERRADUE_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "panel.cloud.cyfronet.pl";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_TERRADUE_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "fedcloud-osservices.egi.cesga.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_TERRADUE_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "cloud-egi.100percentit.com";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_GEODAB_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "identity.cloud.muni.cz";

        monitored = "1";
        production = "1";
        scope = "EGI, elixir, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_OPENUNIVERSE_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "fedcloud-services.egi.cesga.es";

        monitored = "1";
        production = "0";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_OPENUNIVERSE_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "fedcloud-osservices.egi.cesga.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_CLARIN_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "cloud.recas.ba.infn.it";

        monitored = "0";
        production = "0";
        scope = "EGI, wlcg, lhcb, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_CLARIN_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "cloud.recas.ba.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, lhcb, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_CLARIN_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "fedcloud-services.egi.cesga.es";

        monitored = "1";
        production = "0";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_CLARIN_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "identity.cloud.muni.cz";

        monitored = "1";
        production = "1";
        scope = "EGI, elixir, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_CLARIN_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "fedcloud-osservices.egi.cesga.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_DEIMOS_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.accounting";
        hostname = "nova.ui.savba.sk";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_DEIMOS_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "nova.ui.savba.sk";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_DEIMOS_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "fedcloud-services.egi.cesga.es";

        monitored = "1";
        production = "0";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_DEIMOS_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "nova.ui.savba.sk";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_DEIMOS_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "identity.cloud.muni.cz";

        monitored = "1";
        production = "1";
        scope = "EGI, elixir, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_DEIMOS_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.swift";
        hostname = "identity.cloud.muni.cz";

        monitored = "1";
        production = "1";
        scope = "EGI, elixir, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_DEIMOS_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.accounting";
        hostname = "fedcloud-osservices.egi.cesga.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_DEIMOS_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "fedcloud-osservices.egi.cesga.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_EMSOERIC_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "cloud.recas.ba.infn.it";

        monitored = "0";
        production = "0";
        scope = "EGI, wlcg, lhcb, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_EMSOERIC_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "cloud.recas.ba.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, lhcb, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_EMSOERIC_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "fedcloud-services.egi.cesga.es";

        monitored = "1";
        production = "0";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_EMSOERIC_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "fedcloud-osservices.egi.cesga.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_TrainingInfrastructure_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "api.cloud.ifca.es";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, cms, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_TrainingInfrastructure_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "nova3.ui.savba.sk";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_TrainingInfrastructure_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "nova.ui.savba.sk";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_TrainingInfrastructure_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "keystone3.ui.savba.sk";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_TrainingInfrastructure_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "nova.ui.savba.sk";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_TrainingInfrastructure_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "identity.cloud.muni.cz";

        monitored = "1";
        production = "1";
        scope = "EGI, elixir, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_TrainingInfrastructure_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "stack-server.ct.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_Notebooks_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "fedcloud-services.egi.cesga.es";

        monitored = "1";
        production = "0";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_Notebooks_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.notebooks.jupyterhub";
        hostname = "notebooks.egi.eu";

        monitored = "1";
        production = "1";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_Notebooks_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.notebooks.jupyterhub";
        hostname = "aginfra-notebooks.fedcloud-tf.fedcloud.eu";

        monitored = "1";
        production = "1";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_Notebooks_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "stack-server.ct.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_Notebooks_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "fedcloud-osservices.egi.cesga.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_AoD_SLA";
        type = "SERVICEGROUPS";
        service = "Site-BDII";
        hostname = "sitebdii.egi.cesga.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_AoD_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "cream.egi.cesga.es";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_AoD_SLA";
        type = "SERVICEGROUPS";
        service = "SRM";
        hostname = "se2.egi.cesga.es";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_AoD_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "api.cloud.ifca.es";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, cms, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_AoD_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "sbgcloud.in2p3.fr";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, alice, cms, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_AoD_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "openstack.bitp.kiev.ua";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, alice, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_AoD_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-metadata.vmcatcher";
        hostname = "openstack.bitp.kiev.ua";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, alice, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_AoD_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-metadata.vmcatcher";
        hostname = "fc.scai.fraunhofer.de";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_AoD_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "cloud-ctrl.nipne.ro";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_AoD_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "fedcloud-services.egi.cesga.es";

        monitored = "1";
        production = "0";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_AoD_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "thor.univ-lille.fr";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_AoD_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "egi-cloud.pd.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_AoD_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "panel.cloud.cyfronet.pl";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_AoD_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "fc.scai.fraunhofer.de";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_AoD_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "stack-server.ct.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_AoD_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "fedcloud-osservices.egi.cesga.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_AoD_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "cloud-egi.100percentit.com";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_AoD_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-metadata.vmcatcher";
        hostname = "egi.100percentit.com";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_EXTraS_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-metadata.vmcatcher";
        hostname = "egi.cloud.cyfronet.pl";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_EXTraS_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "panel.cloud.cyfronet.pl";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_EXTraS_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "stack-server.ct.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_Fusion_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "fedcloud-services.egi.cesga.es";

        monitored = "1";
        production = "0";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_Fusion_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "thor.univ-lille.fr";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_Fusion_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-metadata.vmcatcher";
        hostname = "thor.univ-lille.fr";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_Fusion_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "fedcloud-osservices.egi.cesga.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "Site-BDII";
        hostname = "bdii-local-grid.obspm.fr";

        monitored = "1";
        production = "1";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "Site-BDII";
        hostname = "sbgbdii1.in2p3.fr";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, alice, cms, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "Site-BDII";
        hostname = "gridiis01.ifca.es";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, cms";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "Site-BDII";
        hostname = "sbdii01.ncg.ingrid.pt";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, atlas, cms";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "Site-BDII";
        hostname = "gridsrv4-4.dir.garr.it";

        monitored = "1";
        production = "1";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "Site-BDII";
        hostname = "gridsite.fe.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, lhcb";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "Site-BDII";
        hostname = "gridsrv-02.roma3.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, atlas";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "Site-BDII";
        hostname = "gridba2.ba.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, alice, atlas, cms, lhcb";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "Site-BDII";
        hostname = "lcg002.ihep.ac.cn";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, atlas, cms";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "cream-ce-grid.obspm.fr";

        monitored = "1";
        production = "1";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "ce05.ncg.ingrid.pt";

        monitored = "0";
        production = "0";
        scope = "EGI, wlcg, tier2, cms";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "ce06.ncg.ingrid.pt";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "gridsrv2-4.dir.garr.it";

        monitored = "1";
        production = "1";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "ce-01.roma3.infn.it";

        monitored = "1";
        production = "0";
        scope = "EGI, wlcg, atlas";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "grid012.ct.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "gridce4.pi.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, cms, lhcb";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "gridce2.pi.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, cms, lhcb";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "gridce3.pi.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, cms, lhcb";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "SRM";
        hostname = "se-dpm-server-grid.obspm.fr";

        monitored = "1";
        production = "1";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "SRM";
        hostname = "sbgse1.in2p3.fr";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, cms";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "SRM";
        hostname = "storm.ifca.es";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, cms";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "SRM";
        hostname = "srm01.ifca.es";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, cms";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "SRM";
        hostname = "srm01.ncg.ingrid.pt";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, atlas, cms";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "SRM";
        hostname = "gridsrv3-4.dir.garr.it";

        monitored = "1";
        production = "1";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "SRM";
        hostname = "grid2.fe.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "SRM";
        hostname = "storm-01.roma3.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, atlas";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "SRM";
        hostname = "prod-se-01.ct.infn.it";

        monitored = "0";
        production = "0";
        scope = "EGI, wlcg, tier2, alice";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "SRM";
        hostname = "storm-se-01.ba.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, cms";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "SRM";
        hostname = "gridsrm.pi.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "SRM";
        hostname = "stormfe1.pi.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, cms";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "org.opensciencegrid.htcondorce";
        hostname = "alict-ce-01.ct.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, alice";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "Site-BDII";
        hostname = "alict-sbdii.ct.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, alice";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "sbgce1.in2p3.fr";

        monitored = "0";
        production = "0";
        scope = "EGI, wlcg, tier2, alice, cms";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "ce-01.recas.ba.infn.it";

        monitored = "0";
        production = "0";
        scope = "EGI, wlcg, tier2, alice, cms, lhcb";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "org.opensciencegrid.htcondorce";
        hostname = "ce-02.recas.ba.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, alice, cms, lhcb";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "sbgcloud.in2p3.fr";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, alice, cms, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "cloud.recas.ba.infn.it";

        monitored = "0";
        production = "0";
        scope = "EGI, wlcg, lhcb, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "cloud.recas.ba.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, lhcb, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "ce03.ncg.ingrid.pt";

        monitored = "0";
        production = "0";
        scope = "EGI, wlcg, atlas";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "ce-03.recas.ba.infn.it";

        monitored = "0";
        production = "0";
        scope = "EGI, wlcg, tier2, cms";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "webdav";
        hostname = "gftp01.ncg.ingrid.pt";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, atlas, cms, EOSC-hub";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "cloud-ctrl.nipne.ro";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "webdav";
        hostname = "gridsrv-06.roma3.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, atlas";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "org.opensciencegrid.htcondorce";
        hostname = "ce-04.recas.ba.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, alice, cms, lhcb";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "CREAM-CE";
        hostname = "ce-02.roma3.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, atlas";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "identity.cloud.muni.cz";

        monitored = "1";
        production = "1";
        scope = "EGI, elixir, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "ARC-CE";
        hostname = "arcce01.ifca.es";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, cms, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "ARC-CE";
        hostname = "arcce02.ifca.es";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, cms, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_LSGC_SLA";
        type = "SERVICEGROUPS";
        service = "webdav";
        hostname = "ccsrm.ihep.ac.cn";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, atlas, cms, lhcb";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_Peachnote_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "identity.cloud.muni.cz";

        monitored = "1";
        production = "1";
        scope = "EGI, elixir, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_OPENBIOMAP_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "api.cloud.ifca.es";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, cms, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_IIASA_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "cloud.recas.ba.infn.it";

        monitored = "0";
        production = "0";
        scope = "EGI, wlcg, lhcb, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_IIASA_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "cloud.recas.ba.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, lhcb, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_EMPHASIS_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "sbgcloud.in2p3.fr";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, alice, cms, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_EMPHASIS_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "panel.cloud.cyfronet.pl";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_EMPHASIS_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "identity.cloud.muni.cz";

        monitored = "1";
        production = "1";
        scope = "EGI, elixir, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_EOSCSYNERGY_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "api.cloud.ifca.es";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, cms, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_EOSCSYNERGY_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "controller.ceta-ciemat.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_EOSCSYNERGY_SLA";
        type = "SERVICEGROUPS";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "nova.ui.savba.sk";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_EOSCSYNERGY_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "nova.ui.savba.sk";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_EOSCSYNERGY_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "identity.cloud.muni.cz";

        monitored = "1";
        production = "1";
        scope = "EGI, elixir, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_EOSCSYNERGY_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "fedcloud-osservices.egi.cesga.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_EOSCSYNERGY_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "stratus.ncg.ingrid.pt";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud, EOSC-hub";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_ENVRIFAIR_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "stack-server.ct.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_ENVRIFAIR_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "fedcloud-osservices.egi.cesga.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_VESPA_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "sbgcloud.in2p3.fr";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, alice, cms, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_VESPA_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "identity.cloud.muni.cz";

        monitored = "1";
        production = "1";
        scope = "EGI, elixir, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_ECRIN_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "cloud.recas.ba.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, lhcb, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_STARS4ALL_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "fedcloud-osservices.egi.cesga.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "EGI_GOSAFE_SLA";
        type = "SERVICEGROUPS";
        service = "org.openstack.nova";
        hostname = "colossus.cesar.unizar.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);
        return list;
    }

    private ArrayList<TopologyEndpointParser.EndpointGroup> createTopologyEndpoint2() {
        ArrayList<TopologyEndpointParser.EndpointGroup> list = new ArrayList<>();
        TopologyEndpointParser parser = new TopologyEndpointParser();

        String group = "GoeGrid";
        String type = "SITES";
        String service = "Site-BDII";
        String hostname = "bdii.goegrid.gwdg.de";

        String monitored = "1";
        String production = "1";
        String scope = "EGI, wlcg, tier2, atlas";
        TopologyEndpointParser.EndpointGroup endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "GoeGrid";
        type = "SITES";
        service = "CREAM-CE";
        hostname = "creamce3.goegrid.gwdg.de";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, atlas";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "GoeGrid";
        type = "SITES";
        service = "CREAM-CE";
        hostname = "creamce2.goegrid.gwdg.de";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, atlas";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "GoeGrid";
        type = "SITES";
        service = "SRM";
        hostname = "se-goegrid.gwdg.de";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, atlas";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "GoeGrid";
        type = "SITES";
        service = "gLite-APEL";
        hostname = "apel.goegrid.gwdg.de";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, atlas";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "GoeGrid";
        type = "SITES";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "occi.cloud.gwdg.de";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "GoeGrid";
        type = "SITES";
        service = "eu.egi.cloud.storage-management.cdmi";
        hostname = "cdmi.cloud.gwdg.de";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "GoeGrid";
        type = "SITES";
        service = "eu.egi.cloud.accounting";
        hostname = "egi.cloud.gwdg.de";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "GoeGrid";
        type = "SITES";
        service = "APEL";
        hostname = "creamce2.goegrid.gwdg.de";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, atlas";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "GoeGrid";
        type = "SITES";
        service = "APEL";
        hostname = "creamce3.goegrid.gwdg.de";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, atlas";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "GoeGrid";
        type = "SITES";
        service = "org.squid-cache.Squid";
        hostname = "squid.goegrid.gwdg.de";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, atlas";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "GoeGrid";
        type = "SITES";
        service = "org.squid-cache.Squid";
        hostname = "squid2.goegrid.gwdg.de";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, atlas";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "GoeGrid";
        type = "SITES";
        service = "CREAM-CE";
        hostname = "creamce.goegrid.gwdg.de";

        monitored = "0";
        production = "0";
        scope = "Local, wlcg, tier2, atlas";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "GoeGrid";
        type = "SITES";
        service = "net.perfSONAR.Bandwidth";
        hostname = "perfsonar01.goegrid.gwdg.de";

        monitored = "0";
        production = "0";
        scope = "EGI, wlcg, tier2, atlas";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "GoeGrid";
        type = "SITES";
        service = "net.perfSONAR.Latency";
        hostname = "perfsonar01.goegrid.gwdg.de";

        monitored = "0";
        production = "0";
        scope = "EGI, wlcg, tier2, atlas";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "GoeGrid";
        type = "SITES";
        service = "ARC-CE";
        hostname = "arcce1.goegrid.gwdg.de";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, atlas";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "GoeGrid";
        type = "SITES";
        service = "ARC-CE";
        hostname = "arcce2.goegrid.gwdg.de";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, tier2, atlas";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "GoeGrid";
        type = "SITES";
        service = "org.openstack.nova";
        hostname = "api.prod.cloud.gwdg.de";

        monitored = "0";
        production = "0";
        scope = "EGI, wlcg, tier2, atlas, SLA";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "GoeGrid";
        type = "SITES";
        service = "eu.egi.cloud.vm-metadata.vmcatcher";
        hostname = "egi.cloud.gwdg.de";

        monitored = "0";
        production = "0";
        scope = "EGI, wlcg, tier2, atlas, SLA";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "CESGA";
        type = "SITES";
        service = "UI";
        hostname = "ui.egi.cesga.es";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "CESGA";
        type = "SITES";
        service = "APEL";
        hostname = "cream.egi.cesga.es";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "CESGA";
        type = "SITES";
        service = "Local-LFC";
        hostname = "lfc.egi.cesga.es";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "CESGA";
        type = "SITES";
        service = "Top-BDII";
        hostname = "topbdii.egi.cesga.es";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "CESGA";
        type = "SITES";
        service = "VOMS";
        hostname = "voms.egi.cesga.es";

        monitored = "1";
        production = "1";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "CESGA";
        type = "SITES";
        service = "Site-BDII";
        hostname = "sitebdii.egi.cesga.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "CESGA";
        type = "SITES";
        service = "MyProxy";
        hostname = "myproxy.egi.cesga.es";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "CESGA";
        type = "SITES";
        service = "CREAM-CE";
        hostname = "cream.egi.cesga.es";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "CESGA";
        type = "SITES";
        service = "SRM";
        hostname = "se2.egi.cesga.es";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "CESGA";
        type = "SITES";
        service = "gLite-APEL";
        hostname = "apel.egi.cesga.es";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "CESGA";
        type = "SITES";
        service = "eu.egi.MPI";
        hostname = "cream.egi.cesga.es";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "CESGA";
        type = "SITES";
        service = "eu.egi.MPI";
        hostname = "cream.egi.cesga.es";

        monitored = "0";
        production = "0";
        scope = "EGI";

        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "CESGA";
        type = "SITES";
        service = "eu.egi.cloud.information.bdii";
        hostname = "ui.egi.cesga.es";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "CESGA";
        type = "SITES";
        service = "emi.ARGUS";
        hostname = "argus.egi.cesga.es";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "CESGA";
        type = "SITES";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "fedcloud-services.egi.cesga.es";

        monitored = "1";
        production = "0";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "CESGA";
        type = "SITES";
        service = "eu.egi.cloud.accounting";
        hostname = "fedcloud-services.egi.cesga.es";

        monitored = "1";
        production = "0";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "CESGA";
        type = "SITES";
        service = "eu.egi.cloud.accounting";
        hostname = "fedcloud-cmdone.egi.cesga.es";

        monitored = "0";
        production = "0";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "CESGA";
        type = "SITES";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "fedcloud-cmdone.egi.cesga.es";

        monitored = "0";
        production = "0";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "CESGA";
        type = "SITES";
        service = "eu.egi.storage.accounting";
        hostname = "se2.egi.cesga.es";

        monitored = "0";
        production = "0";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "CESGA";
        type = "SITES";
        service = "eu.egi.cloud.accounting";
        hostname = "fedcloud-osservices.egi.cesga.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "CESGA";
        type = "SITES";
        service = "org.openstack.nova";
        hostname = "fedcloud-osservices.egi.cesga.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "CESGA";
        type = "SITES";
        service = "VOMS";
        hostname = "voms2.egi.cesga.es";

        monitored = "1";
        production = "1";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "UPV-GRyCAP";
        type = "SITES";
        service = "Site-BDII";
        hostname = "ngiesbdii.i3m.upv.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "UPV-GRyCAP";
        type = "SITES";
        service = "eu.egi.cloud.accounting";
        hostname = "fc-one.i3m.upv.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "UPV-GRyCAP";
        type = "SITES";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "fc-one.i3m.upv.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "UPV-GRyCAP";
        type = "SITES";
        service = "es.upv.grycap.im";
        hostname = "appsgrycap.i3m.upv.es";

        monitored = "0";
        production = "0";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "UPV-GRyCAP";
        type = "SITES";
        service = "eu.egi.cloud.accounting";
        hostname = "fc-horsemen.i3m.upv.es";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud, SLA";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "RECAS-BARI";
        type = "SITES";
        service = "Site-BDII";
        hostname = "cloud-bdii.recas.ba.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, lhcb, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "RECAS-BARI";
        type = "SITES";
        service = "eu.egi.cloud.vm-management.occi";
        hostname = "cloud.recas.ba.infn.it";

        monitored = "0";
        production = "0";
        scope = "EGI, wlcg, lhcb, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "RECAS-BARI";
        type = "SITES";
        service = "org.openstack.nova";
        hostname = "cloud.recas.ba.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, lhcb, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "RECAS-BARI";
        type = "SITES";
        service = "eu.egi.cloud.accounting";
        hostname = "cloud-accounting.recas.ba.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, wlcg, lhcb, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "RECAS-BARI";
        type = "SITES";
        service = "VOMS";
        hostname = "voms.recas.ba.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "INFN-CATANIA-STACK";
        type = "SITES";
        service = "eu.egi.cloud.accounting";
        hostname = "indigo-sb.ct.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "INFN-CATANIA-STACK";
        type = "SITES";
        service = "Site-BDII";
        hostname = "stack-server.ct.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";
        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        group = "INFN-CATANIA-STACK";
        type = "SITES";
        service = "org.openstack.nova";
        hostname = "stack-server.ct.infn.it";

        monitored = "1";
        production = "1";
        scope = "EGI, FedCloud";

        endpGroup = parser.new EndpointGroup(group, hostname, service, type, parser.new Tags(scope, production, monitored));
        list.add(endpGroup);

        return list;
    }

    private JSONObject readJsonFromFile(String path) throws FileNotFoundException, IOException, org.json.simple.parser.ParseException {
        JSONParser parser = new JSONParser();
        URL url = TimelineMergerTest.class.getResource(path);
        Object obj = parser.parse(new FileReader(path));

        JSONObject jsonObject = (JSONObject) obj;

        return jsonObject;
    }
}
