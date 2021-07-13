package ops;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import org.junit.BeforeClass;
import org.junit.Test;

public class ThresholdManagerTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", ThresholdManagerTest.class.getResource("/ops/EGI-algorithm.json"));
		assertNotNull("Test file missing", ThresholdManagerTest.class.getResource("/ops/EGI-rules.json"));
	}

	@Test
	public void test() throws IOException, URISyntaxException {

		// Prepare Resource File
		URL opsJsonFile = ThresholdManagerTest.class.getResource("/ops/EGI-algorithm.json");
		File opsFile = new File(opsJsonFile.toURI());
		// Instantiate class
		OpsManager opsMgr = new OpsManager();
		// Test loading file
		opsMgr.loadJson(opsFile);

		// Prepare Resource File
		URL thrJsonFile = ThresholdManagerTest.class.getResource("/ops/EGI-rules.json");
		File thrFile = new File(thrJsonFile.toURI());
		// Instantiate class
		ThresholdManager t = new ThresholdManager();
		t.parseJSONFile(thrFile);

		String[] expectedRules = new String[] { "//org.bdii.Freshness", "//org.bdii.Entries",
				"/bdii.host1.example.foo/org.bdii.Freshness", "/bdii.host3.example.foo/org.bdii.Freshness",
				"SITE-101/bdii.host1.example.foo/org.bdii.Freshness", "SITE-101//org.bdii.Freshness" };

		assertEquals(expectedRules.length, t.getRules().entrySet().size());

		for (String rule : expectedRules) {
			assertEquals(true, t.getRules().keySet().contains(rule));
		}

		assertEquals("SITE-101/bdii.host1.example.foo/org.bdii.Freshness",
				t.getMostRelevantRule("SITE-101", "bdii.host1.example.foo", "org.bdii.Freshness"));

		assertEquals("SITE-101//org.bdii.Freshness",
				t.getMostRelevantRule("SITE-101", "bdii.host2.example.foo", "org.bdii.Freshness"));

		assertEquals("//org.bdii.Freshness",
				t.getMostRelevantRule("SITE-202", "bdii.host2.example.foo", "org.bdii.Freshness"));

		assertEquals("//org.bdii.Freshness",
				t.getMostRelevantRule("SITE-202", "bdii.host2.example.foo", "org.bdii.Freshness"));

		assertEquals("//org.bdii.Entries",
				t.getMostRelevantRule("SITE-101", "bdii.host1.example.foo", "org.bdii.Entries"));

		assertEquals("", t.getMostRelevantRule("SITE-101", "bdii.host1.example.foo", "org.bdii.Foo"));

		assertEquals("WARNING", t.getStatusByRule("SITE-101/bdii.host1.example.foo/org.bdii.Freshness", opsMgr, "AND"));
		assertEquals("CRITICAL", t.getStatusByRule("//org.bdii.Entries", opsMgr, "AND"));
		assertEquals("WARNING", t.getStatusByRule("//org.bdii.Entries", opsMgr, "OR"));
		assertEquals("CRITICAL", t.getStatusByRule("/bdii.host1.example.foo/org.bdii.Freshness", opsMgr, "AND"));
		assertEquals("WARNING", t.getStatusByRule("/bdii.host1.example.foo/org.bdii.Freshness", opsMgr, "OR"));
		assertEquals("",t.getStatusByRule("/bdii.host3.example.foo/org.bdii.Freshness", opsMgr, "AND")); //no critical or warning ranges defined
		
	}

}
