package ops;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;

import org.junit.BeforeClass;
import org.junit.Test;

import sync.EndpointGroups;
import sync.EndpointGroupsTest;
import sync.MetricProfilesTest;

public class OpsManagerTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", OpsManagerTest.class.getResource("/ops/EGI-algorithm.json"));
	}

	@Test
	public void test() throws URISyntaxException, IOException {
		// Prepare Resource File
		URL resJsonFile = OpsManagerTest.class.getResource("/ops/EGI-algorithm.json");
		File JsonFile = new File(resJsonFile.toURI());
		// Instatiate class
		OpsManager opsMgr = new OpsManager();
		// Test loading file
		opsMgr.loadJson(JsonFile);

		// Test the available states
		ArrayList<String> avStates = new ArrayList<String>();
		avStates.add("OK");
		avStates.add("WARNING");
		avStates.add("UNKNOWN");
		avStates.add("MISSING");
		avStates.add("CRITICAL");
		avStates.add("DOWNTIME");

		assertEquals("Retrieve Available States", opsMgr.availableStates(), avStates);

		// Test the available operations
		ArrayList<String> avOps = new ArrayList<String>();
		avOps.add("AND");
		avOps.add("OR");
		assertEquals("Retrieve Available Operations", opsMgr.availableOps(), avOps);

		// Test the available operations on a variety of states
		assertEquals("OK (OR) OK = OK", opsMgr.op("OR", "OK", "OK"), "OK");
		assertEquals("OK (OR) CRITICAL = OK", opsMgr.op("OR", "CRITICAL", "OK"), "OK");
		assertEquals("CRITICAL (OR) MISSING = CRITICAL", opsMgr.op("OR", "CRITICAL", "MISSING"), "CRITICAL");
		assertEquals("WARNING (OR) MISSING = WARNING", opsMgr.op("OR", "WARNING", "MISSING"), "WARNING");
		assertEquals("WARNING (AND) MISSING = MISSING", opsMgr.op("AND", "WARNING", "MISSING"), "MISSING");
		assertEquals("OK (AND) CRITICAL = CRITICAL", opsMgr.op("AND", "OK", "CRITICAL"), "CRITICAL");
		assertEquals("DOWNTIME (AND) UNKNOWN = DOWNTIME", opsMgr.op("AND", "DOWNTIME", "UNKNOWN"), "DOWNTIME");

		assertEquals("Default Downtime Status = DOWNTIME", opsMgr.getDefaultDown(), "DOWNTIME");
		System.out.println(opsMgr.getDefaultMissingInt());
	}

}
