package sync;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;

import ops.ConfigManager;

import org.junit.BeforeClass;
import org.junit.Test;

public class EndpointGroupManagerTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", EndpointGroupManagerTest.class.getResource("/avro/group_endpoints_v2.avro"));
	}

	@Test
	public void test() throws URISyntaxException, IOException {
		// Prepare Resource File
		URL resAvroFile = EndpointGroupManagerTest.class.getResource("/avro/group_endpoints_v2.avro");
		File avroFile = new File(resAvroFile.toURI());
		// Instatiate class
		EndpointGroupManager ge = new EndpointGroupManager();
		// Test loading file
		ge.loadAvro(avroFile);
		assertNotNull("File Loaded", ge);

		// Test Check if service endpoint exists in topology
		assertTrue(ge.checkEndpoint("storage1.grid.upjs.sk", "ARC-CE"));
		assertTrue(ge.checkEndpoint("storage1.grid.upjs.sk", "ARC-CE"));
		assertTrue(ge.checkEndpoint("se01.afroditi.hellasgrid.gr", "SRMv2"));
		assertTrue(ge.checkEndpoint("grid-perfsonar.hpc.susx.ac.uk", "net.perfSONAR.Latency"));
		assertTrue(ge.checkEndpoint("se.grid.tuke.sk", "SRMv2"));
		assertTrue(ge.checkEndpoint("dpm.grid.atomki.hu", "SRMv2"));
		// Test check Group retrieval
		ArrayList<String> result1 = new ArrayList<String>();
		result1.add("ru-PNPI");
		assertEquals(ge.getGroup("SITES", "gt3.pnpi.nw.ru", "CREAM-CE"), result1);

		// Test Tag Filtering (Wont filter out anything since input is already
		// filtered)
		URL resJson = GroupsOfGroupsTest.class.getResource("/ops/config.json");
		File cfgFile = new File(resJson.toURI());
		ConfigManager cfgMgr = new ConfigManager();
		cfgMgr.loadJson(cfgFile);
		ge.filter(cfgMgr.egroupTags);

		// Check non-existent groups
		assertTrue(ge.checkEndpoint("ce.etfos.cro-ngi.hr", "GRAM5") == false);
		assertTrue(ge.checkEndpoint("grid129.sinp.msu.ru", "CREAM-CE") == false);

	}

}
