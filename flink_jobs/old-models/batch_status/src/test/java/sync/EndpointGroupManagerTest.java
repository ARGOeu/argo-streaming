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
		assertNotNull("Test file missing", EndpointGroupManagerTest.class.getResource("/avro/group_endpoints_info.avro"));
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
		URL resJson = EndpointGroupManagerTest.class.getResource("/ops/config.json");
		File cfgFile = new File(resJson.toURI());
		ConfigManager cfgMgr = new ConfigManager();
		cfgMgr.loadJson(cfgFile);
		ge.filter(cfgMgr.egroupTags);

		// Check non-existent groups
		assertTrue(ge.checkEndpoint("ce.etfos.cro-ngi.hr", "GRAM5") == false);
		assertTrue(ge.checkEndpoint("grid129.sinp.msu.ru", "CREAM-CE") == false);
		
		// Prepare Resource File with extra information in tags
		URL resAvroFile2 = EndpointGroupManagerTest.class.getResource("/avro/group_endpoints_info.avro");
		File avroFile2 = new File(resAvroFile2.toURI());
		// Instantiate class
		EndpointGroupManager ge2 = new EndpointGroupManager();
		// Test loading file
		ge2.loadAvro(avroFile2);
		assertNotNull("File Loaded", ge);
		
		String exp1 = "URL:host1.example.foo/path/to/service1,DN:foo DN";
		String exp2 = "URL:host1.example.foo/path/to/service2";
		String exp3 = "URL:host2.example.foo/path/to/service1";
		String exp4 = "ext.Value:extension1,URL:host2.example.foo/path/to/service2";
		String exp5 = "";
		String exp6 = "URL:host4.example.foo/path/to/service1";
		
		assertEquals("wrong tags", exp1,ge2.getInfo("groupA", "SERVICEGROUPS", "host1.example.foo_11", "services.url"));
		assertEquals("wrong tags", exp2,ge2.getInfo("groupB", "SERVICEGROUPS", "host1.example.foo_22", "services.url"));
		assertEquals("wrong tags", exp3,ge2.getInfo("groupC", "SERVICEGROUPS", "host2.example.foo_33", "services.url"));
		assertEquals("wrong tags", exp4,ge2.getInfo("groupD", "SERVICEGROUPS", "host2.example.foo_44", "services.url"));
		assertEquals("wrong tags", exp5,ge2.getInfo("groupE", "SERVICEGROUPS", "host3.example.foo_55", "services.url"));
		assertEquals("wrong tags", exp6,ge2.getInfo("groupF", "SERVICEGROUPS", "host4.example.foo_66", "services.url"));
	}

}
