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
		
		// Test parsing of label=value lists including space separation or not
		assertEquals("{size=6754.0, time=3.714648}",t.getThresholdValues("time=3.714648s;;;0.000000 size=6754B;;;0").toString());
		assertEquals("{time=0.037908}",t.getThresholdValues("time=0.037908s;;;0.000000;120.000000").toString());
		assertEquals("{time=0.041992}",t.getThresholdValues("time=0.041992s;;;0.000000;120.000000").toString());
		assertEquals("{entries=1.0, time=0.15}",t.getThresholdValues("time=0.15s;entries=1").toString());
		assertEquals("{entries=1.0, freshness=111.0}",t.getThresholdValues("freshness=111s;entries=1").toString());
		assertEquals("{entries=1.0, freshness=111.0}",t.getThresholdValues("freshness=111s; entries=1").toString());
		assertEquals("{entries=1.0, freshness=111.0}",t.getThresholdValues("freshness=111s;;;entries=1").toString());
		assertEquals("{entries=1.0, freshness=111.0}",t.getThresholdValues("freshness=111s;;    entries=1").toString());
		assertEquals("{TSSInstances=1.0}",t.getThresholdValues("TSSInstances=1").toString());
		
		String thBig = "tls_ciphers=105.47s dir_head=0.69s dir_get=0.89s file_put=0.82s file_get=0.45s file_options=0.39s file_move=0.42s file_head=0.40s file_head_on_non_existent=0.38s file_propfind=0.40s file_delete=0.72s file_delete_on_non_existent=0.37s";
		String expThBig = "{file_head_on_non_existent=0.38, file_put=0.82, file_delete_on_non_existent=0.37, file_delete=0.72, dir_head=0.69, file_head=0.4, file_propfind=0.4, dir_get=0.89, file_move=0.42, file_options=0.39, file_get=0.45, tls_ciphers=105.47}";
		
		assertEquals(expThBig,t.getThresholdValues(thBig).toString());
	}

}
