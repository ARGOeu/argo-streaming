package sync;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;

import ops.ConfigManager;

import org.junit.BeforeClass;
import org.junit.Test;

public class GroupsOfGroupsTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", GroupsOfGroupsTest.class.getResource("/avro/group_groups_v2.avro"));
	}

	@Test
	public void test() throws URISyntaxException, IOException {
		// Prepare Resource File
		URL resAvroFile = GroupsOfGroupsTest.class.getResource("/avro/group_groups_v2.avro");
		File avroFile = new File(resAvroFile.toURI());
		// Instatiate class
		GroupGroupManager gg = new GroupGroupManager();
		// Test loading file
		gg.loadAvro(avroFile);
		assertNotNull("File Loaded", gg);
		// Test retrieve group by subgroup name and group type
		assertEquals(gg.getGroup("NGI", "UNI-BONN"), "NGI_DE");
		assertEquals(gg.getGroup("NGI", "MSFG-OPEN"), "NGI_FRANCE");
		assertEquals(gg.getGroup("NGI", "HG-02-IASA"), "NGI_GRNET");
		assertEquals(gg.getGroup("NGI", "ZA-MERAKA"), "AfricaArabia");
		assertEquals(gg.getGroup("NGI", "RU-SPbSU"), "Russia");
		// Test to assert if groups exist
		assertTrue(gg.checkSubGroup("UNI-BONN"));
		assertTrue(gg.checkSubGroup("MSFG-OPEN"));
		assertTrue(gg.checkSubGroup("HG-02-IASA"));
		assertTrue(gg.checkSubGroup("ZA-MERAKA"));
		assertTrue(gg.checkSubGroup("RU-SPbSU"));

		// Test Tag Filtering (Wont filter out anything since input is already
		// filtered)
		URL resJson = GroupsOfGroupsTest.class.getResource("/ops/config.json");
		File cfgFile = new File(resJson.toURI());
		ConfigManager cfgMgr = new ConfigManager();
		cfgMgr.loadJson(cfgFile);
		gg.filter(cfgMgr.ggroupTags);

		// Test groups that are not present
		assertNotEquals(gg.getGroup("NGI", "KE-UONBI-01"), "AfricaArabia");
		assertNotEquals(gg.getGroup("NGI", "RU-Novosibirsk-BINP"), "Russia");
		assertTrue(gg.checkSubGroup("FRANCE-GRILLES-TESTBED") == false);

		// Test exceptions

	}

}
