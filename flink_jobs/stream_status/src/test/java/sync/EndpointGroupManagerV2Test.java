package sync;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import ops.ConfigManager;
import sync.EndpointGroupManagerV2.EndpointItem;

import org.junit.BeforeClass;
import org.junit.Test;

import argo.avro.GroupEndpoint;
import argo.avro.MetricData;

public class EndpointGroupManagerV2Test {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing",
				EndpointGroupManagerV2Test.class.getResource("/avro/group_endpoints_v2.avro"));
	}

	@Test
	public void test() throws URISyntaxException, IOException {
		// Prepare Resource File
		URL resAvroFile = EndpointGroupManagerV2Test.class.getResource("/avro/group_endpoints_v2.avro");
		File avroFile = new File(resAvroFile.toURI());
		// Instantiate class
		EndpointGroupManagerV2 ge = new EndpointGroupManagerV2();
		// Test loading file 2
		ge.loadAvro(avroFile);
		
		
		
		
		assertNotNull("File Loaded", ge);

		// Test Check if service endpoint exists in topology
		assertTrue(ge.checkEndpoint("storage1.grid.upjs.sk", "ARC-CE"));
		assertTrue(ge.checkEndpoint("storage1.grid.upjs.sk", "ARC-CE"));
		assertTrue(ge.checkEndpoint("se01.afroditi.hellasgrid.gr", "SRMv2"));
		assertTrue(ge.checkEndpoint("grid-perfsonar.hpc.susx.ac.uk", "net.perfSONAR.Latency"));
		assertTrue(ge.checkEndpoint("se.grid.tuke.sk", "SRMv2"));
		assertTrue(ge.checkEndpoint("dpm.grid.atomki.hu", "SRMv2"));
		assertTrue(ge.checkEndpoint("gt3.pnpi.nw.ru", "CREAM-CE"));
		
	
		
		// Test check Group retrieval
		ArrayList<String> result1 = new ArrayList<String>();
		result1.add("ru-PNPI");
		assertEquals(ge.getGroup( "gt3.pnpi.nw.ru", "CREAM-CE"), result1);
		ArrayList<String> l = ge.getGroup("gt3.pnpi.nw.ru", "CREAM-CE");
		

		// Check non-existent groups
		assertTrue(ge.checkEndpoint("ce.etfos.cro-ngi.hr", "GRAM5") == false);
		assertTrue(ge.checkEndpoint("grid129.sinp.msu.ru", "CREAM-CE") == false);
		
		ArrayList<GroupEndpoint> egpList = new ArrayList<GroupEndpoint>();
		for (int i=0; i<5; i++){
			GroupEndpoint itemNew = new GroupEndpoint();
			itemNew.setGroup("FOO");
			itemNew.setHostname("host_"+Integer.toString(i));
			itemNew.setType("SITE");
			egpList.add(itemNew);
		}
		
		EndpointGroupManagerV2 egpMgr = new EndpointGroupManagerV2();
		egpMgr.loadFromList(egpList);
		
		assertTrue(egpMgr.getList().size()==5);
		
		
	
	}

}
