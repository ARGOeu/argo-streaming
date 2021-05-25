/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.commons.profiles;

import argo.commons.timelines.TimelineMergerTest;
import argo.commons.timelines.Utils;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
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
public class DowntimeParserTest {

    private String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    public DowntimeParserTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        assertNotNull("Test file missing", DowntimeParserTest.class.getResource("/profiles/downtimes.json"));
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
     * Test of getDowntimeEndpoints method, of class DowntimeParser.
     */
    @Test
    public void testGetDowntimeEndpoints() throws IOException, ParseException, org.json.simple.parser.ParseException {
        System.out.println("getDowntimeEndpoints");

        JSONObject downJSONObject = readJsonFromFile(DowntimeParserTest.class.getResource("/profiles/downtimes.json").getFile());
        DowntimeParser instance = new DowntimeParser(downJSONObject);
        ArrayList<DowntimeParser.Endpoint> expResult = createEndpoints();
        ArrayList<DowntimeParser.Endpoint> result = instance.getDowntimeEndpoints();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    /**
     * Test of setDowntimeEndpoints method, of class DowntimeParser.
     */
    @Test
    public void testSetDowntimeEndpoints() throws IOException, org.json.simple.parser.ParseException, ParseException {
        System.out.println("setDowntimeEndpoints");
        ArrayList<DowntimeParser.Endpoint> downtimeEndpoints = new ArrayList<>();
        JSONObject downJSONObject = readJsonFromFile(DowntimeParserTest.class.getResource("/profiles/downtimes.json").getFile());
        DowntimeParser instance = new DowntimeParser(downJSONObject);
        instance.setDowntimeEndpoints(downtimeEndpoints);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");

    }

    private ArrayList<DowntimeParser.Endpoint> createEndpoints() throws ParseException, IOException, FileNotFoundException, org.json.simple.parser.ParseException {

        ArrayList<DowntimeParser.Endpoint> endpoints = new ArrayList<>();
        JSONObject downJSONObject = readJsonFromFile(DowntimeParserTest.class.getResource("/profiles/downtimes.json").getFile());
        DowntimeParser downtimeparser = new DowntimeParser(downJSONObject);
        String[] endp1 = new String[]{"lhcb-ce.nipne.ro", "APEL", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        DowntimeParser.Endpoint endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"lhcb-ce.nipne.ro", "Site-BDII", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"lhcb-ce.nipne.ro", "CREAM-CE", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"lhcb-ce.nipne.ro", "gLExec", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"lhcb-perf.nipne.ro", "net.perfSONAR.Bandwidth", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"lhcb-ce.nipne.ro", "gLite-APEL", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"ce01.igfae.usc.es", "APEL", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"ce01.igfae.usc.es", "CREAM-CE", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"ce01.igfae.usc.es", "gLExec", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"ce.scope.unina.it", "APEL", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"emi-ce01.scope.unina.it", "CREAM-CE", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"ce.scope.unina.it", "CREAM-CE", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"ce.scope.unina.it", "eu.egi.MPI", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"emi-ce01.scope.unina.it", "eu.egi.MPI", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"emi-ce01.scope.unina.it", "APEL", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"grisuce.scope.unina.it", "APEL", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"grisuce.scope.unina.it", "CREAM-CE", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"grisuce.scope.unina.it", "eu.egi.MPI", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"recasna-ce01.unina.it", "APEL", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"recasna-ce01.unina.it", "CREAM-CE", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"recas-ce02.na.infn.it", "CREAM-CE", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"recas-ce02.na.infn.it", "APEL", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"recas-ce-01.cs.infn.it", "CREAM-CE", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"recasna-ce02.unina.it", "CREAM-CE", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"recasna-ce02.unina.it", "APEL", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"cream.tryton.task.gda.pl", "CREAM-CE", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"foam.grid.kiae.ru", "APEL", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"cow.grid.kiae.ru", "Top-BDII", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"pusher.grid.kiae.ru", "Top-BDII", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"house.grid.kiae.ru", "VO-box", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"gate.grid.kiae.ru", "Site-BDII", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"fate.grid.kiae.ru", "CREAM-CE", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"foam.grid.kiae.ru", "CREAM-CE", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"octopus.grid.kiae.ru", "LB", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"se.grid.kiae.ru", "SRM", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"rnag1.grid.kiae.ru", "ngi.SAM", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"lemon.grid.kiae.ru", "gLite-APEL", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"btw-bw.grid.kiae.ru", "net.perfSONAR.Bandwidth", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"btw-lat.grid.kiae.ru", "net.perfSONAR.Latency", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"cash1.grid.kiae.ru", "org.squid-cache.Squid", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"cash2.grid.kiae.ru", "org.squid-cache.Squid", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"gridce.ilc.cnr.it", "APEL", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"gridbdii.ilc.cnr.it", "Site-BDII", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"gridse.ilc.cnr.it", "SRM", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"gridce.ilc.cnr.it", "gLite-APEL", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"gridse.ilc.cnr.it", "eu.egi.storage.accounting", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"gridse.ilc.cnr.it", "webdav", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"gridse.ilc.cnr.it", "globus-GRIDFTP", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"linucs-ce-01.cs.infn.it", "APEL", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"linucs-ce-01.cs.infn.it", "CREAM-CE", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"recas-ce-02.cs.infn.it", "CREAM-CE", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"recas-ce-02.cs.infn.it", "APEL", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"sbdii-egi.cesar.unizar.es", "Site-BDII", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"colossus.cesar.unizar.es", "org.openstack.nova", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"sbdii-egi.cesar.unizar.es", "eu.egi.cloud.accounting", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"sbdii-egi.cesar.unizar.es", "eu.egi.cloud.information.bdii", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"sbdii-egi.cesar.unizar.es", "eu.egi.cloud.infoProvider", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"sbdii-egi.cesar.unizar.es", "eu.egi.cloud.vm-metadata.vmcatcher", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"node02.datagrid.cea.fr", "UI", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"pre7230.datagrid.cea.fr", "ARC-CE", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"node12.datagrid.cea.fr", "SRM", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"perfsonar02.datagrid.cea.fr", "net.perfSONAR.Bandwidth", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"perfsonar01.datagrid.cea.fr", "net.perfSONAR.Latency", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"node12.datagrid.cea.fr", "XRootD.Redirector", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"node56.datagrid.cea.fr", "gLite-APEL", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"node09.datagrid.cea.fr", "VO-box", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"node10.datagrid.cea.fr", "UI", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"node04.datagrid.cea.fr", "ngi.ARGUS", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"node21.datagrid.cea.fr", "Top-BDII", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"node23.datagrid.cea.fr", "Site-BDII", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"node16.datagrid.cea.fr", "ARC-CE", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"node12.datagrid.cea.fr", "webdav", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"node12.datagrid.cea.fr", "eu.egi.storage.accounting", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"node24.datagrid.cea.fr", "org.squid-cache.Squid", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"node25.datagrid.cea.fr", "org.squid-cache.Squid", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"castorlhcb.cern.ch", "XRootD", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"srm-lhcb.cern.ch", "SRM.nearline", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"ce1.ts.infn.it", "APEL", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"gridbdii.ts.infn.it", "Site-BDII", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"ce1.ts.infn.it", "CREAM-CE", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"gridsrm.ts.infn.it", "SRM", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"gridapel.ts.infn.it", "gLite-APEL", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"farmsrv.ts.infn.it", "org.squid-cache.Squid", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"ce2.ts.infn.it", "org.opensciencegrid.htcondorce", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"ui.hep.phy.cam.ac.uk", "UI", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"serv04.hep.phy.cam.ac.uk", "net.perfSONAR.Bandwidth", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"serv04.hep.phy.cam.ac.uk", "net.perfSONAR.Latency", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"vserv05.hep.phy.cam.ac.uk", "gLite-APEL", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"squid.hep.phy.cam.ac.uk", "org.squid-cache.Squid", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"vac1.hep.phy.cam.ac.uk", "uk.ac.gridpp.vac", "2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"lapp-site01.in2p3.fr", "Site-BDII", "2021-03-10T08:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"lapp-ps01.in2p3.fr", "net.perfSONAR.Bandwidth", "2021-03-10T08:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"lapp-ps02.in2p3.fr", "net.perfSONAR.Latency", "2021-03-10T08:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"lapp-se01.in2p3.fr", "XRootD", "2021-03-10T08:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"lapp-argus02.in2p3.fr", "emi.ARGUS", "2021-03-10T08:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"lapp-squid02.in2p3.fr", "org.squid-cache.Squid", "2021-03-10T08:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"lapp-squid03.in2p3.fr", "org.squid-cache.Squid", "2021-03-10T08:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"lapp-se01.in2p3.fr", "eu.egi.storage.accounting", "2021-03-10T08:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"lapp-bdii01.in2p3.fr", "Top-BDII", "2021-03-10T08:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"lapp-se01.in2p3.fr", "webdav", "2021-03-10T08:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"lapp-ce04.in2p3.fr", "ARC-CE", "2021-03-10T08:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"lapp-ce05.in2p3.fr", "ARC-CE", "2021-03-10T08:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"lapp-ce04.in2p3.fr", "APEL", "2021-03-10T08:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);
        endp1 = new String[]{"lapp-ce06.in2p3.fr", "ARC-CE", "2021-03-10T08:00:00Z", "2021-03-10T23:59:00Z"};
        endpoint = downtimeparser.new Endpoint(endp1[0], endp1[1], Utils.convertStringtoDate(format, endp1[2]), Utils.convertStringtoDate(format, endp1[3]));
        endpoints.add(endpoint);

        return endpoints;

    }

    /**
     * Test of retrieveDownTimes method, of class DowntimeParser.
     */
    @Test
    public void testRetrieveDownTimes() throws IOException, org.json.simple.parser.ParseException, ParseException {
        System.out.println("retrieveDownTimes");
        String hostname = "gridapel.ts.infn.it";
        String service = "gLite-APEL";
        ArrayList<DowntimeParser.Endpoint> endpoints = new ArrayList<>();
        JSONObject downJSONObject = readJsonFromFile(DowntimeParserTest.class.getResource("/profiles/downtimes.json").getFile());
        DowntimeParser instance = new DowntimeParser(downJSONObject);
   
        String[] expResult = new String[]{"2021-03-10T00:00:00Z", "2021-03-10T23:59:00Z"};

        String[] result = instance.retrieveDownTimes(hostname, service);
        assertArrayEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }

    private JSONObject readJsonFromFile(String path) throws FileNotFoundException, IOException, org.json.simple.parser.ParseException {
        JSONParser parser = new JSONParser();
        Object obj = parser.parse(new FileReader(path));

        JSONObject jsonObject = (JSONObject) obj;

        return jsonObject;
    }
}
