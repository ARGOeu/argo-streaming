package profilesmanager;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;

public class DowntimeManagerTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", DowntimeManagerTest.class.getResource("/profiles/downtimes_v2.avro"));
	}

	@Test
	public void test() throws IOException, URISyntaxException {
		// Prepare Resource File
		URL resAvroFile = DowntimeManagerTest.class.getResource("/profiles/downtimes_v2.avro");
		File avroFile = new File(resAvroFile.toURI());
		// Instantiate class
		DowntimeManager dt = new DowntimeManager();
		// Test loading file
		dt.loadAvro(avroFile);
		assertNotNull("File Loaded", dt);

		// Test time period retrieval by service endpoint

		// test for cream-ce01.gridpp.rl.ac.uk CREAM-CE
		ArrayList<String> timePeriod = new ArrayList<String>();
		timePeriod.add("2015-05-07T00:00:00Z");
		timePeriod.add("2015-05-07T23:59:00Z");
			assertEquals("Test timeperiod #1", Arrays.asList(dt.getPeriod("cream-ce01.gridpp.rl.ac.uk", "CREAM-CE").get(0)), timePeriod);// test for px.ire.kharkov.ua, MyProxy
		timePeriod.clear();
		timePeriod.add("2015-05-07T00:00:00Z");
		timePeriod.add("2015-05-07T23:59:00Z");
	assertEquals("Test timeperiod #2",Arrays.asList(dt.getPeriod("px.ire.kharkov.ua", "MyProxy").get(0)), timePeriod);	// test for gb-ui-nki.els.sara.nl, UI
		timePeriod.clear();
		timePeriod.add("2015-05-07T00:00:00Z");
		timePeriod.add("2015-05-07T23:59:00Z");
		assertEquals("Test timeperiod #3", Arrays.asList(dt.getPeriod("gb-ui-nki.els.sara.nl", "UI").get(0)), timePeriod);
		// test for cream-ce01.gridpp.rl.ac.uk, gLExec
		timePeriod.clear();
		timePeriod.add("2015-05-07T00:00:00Z");
		timePeriod.add("2015-05-07T23:59:00Z");
		assertEquals("Test timeperiod #4", Arrays.asList(dt.getPeriod("cream-ce01.gridpp.rl.ac.uk", "gLExec").get(0)), timePeriod);
		// test for gcvmfs.cat.cbpf.br, org.squid-cache.Squid
		timePeriod.clear();
		timePeriod.add("2015-05-07T00:00:00Z");
		timePeriod.add("2015-05-07T20:00:00Z");
		assertEquals("Test timeperiod #5", Arrays.asList(dt.getPeriod("cvmfs.cat.cbpf.br", "org.squid-cache.Squid").get(0)), timePeriod);
	// test for apel.ire.kharkov.ua, APEL
		timePeriod.clear();
		timePeriod.add("2015-05-07T00:00:00Z");
		timePeriod.add("2015-05-07T23:59:00Z");
		assertEquals("Test timeperiod #6", Arrays.asList(dt.getPeriod("apel.ire.kharkov.ua", "APEL").get(0)), timePeriod);

	}

}