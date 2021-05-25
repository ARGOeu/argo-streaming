package ops;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import org.junit.BeforeClass;
import org.junit.Test;

public class ConfigManagerTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", ConfigManagerTest.class.getResource("/ops/config.json"));
	}

	@Test
	public void test() throws URISyntaxException, IOException {
		// Load the resource file
		URL resJsonFile = OpsManagerTest.class.getResource("/ops/config.json");
		File jsonFile = new File(resJsonFile.toURI());

		// Instantiate a new ConfigManager and load the test file
		ConfigManager cfgMgr = new ConfigManager();
		cfgMgr.loadJson(jsonFile);

		// Assert that the simple fields are loaded correctly
		assertEquals("EGI", cfgMgr.tenant);
		assertEquals("Critical", cfgMgr.report);
		assertEquals("SITES", cfgMgr.egroup);
		assertEquals("NGI", cfgMgr.ggroup);
		assertEquals("hepspec", cfgMgr.weight);
		assertEquals("c800846f-8478-4af8-85d1-a3f12fe4c18f",cfgMgr.id);

		// Assert compound fields
		assertEquals("Production", cfgMgr.ggroupTags.get("infrastructure"));
		assertEquals("Certified", cfgMgr.ggroupTags.get("certification"));
		assertEquals("EGI", cfgMgr.ggroupTags.get("scope"));

		// Assert compound fields
		assertEquals("1", cfgMgr.egroupTags.get("production"));
		assertEquals("1", cfgMgr.egroupTags.get("monitored"));
		assertEquals("EGI", cfgMgr.egroupTags.get("scope"));

		// Assert compound fields
		assertEquals("ops", cfgMgr.mdataTags.get("vo"));
		assertEquals("ops", cfgMgr.mdataTags.get("vo_fqan"));
		assertEquals("any", cfgMgr.mdataTags.get("roc"));


	}

}
