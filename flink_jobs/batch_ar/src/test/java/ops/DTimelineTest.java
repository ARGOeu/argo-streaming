package ops;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.junit.BeforeClass;
import org.junit.Test;

public class DTimelineTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", DTimelineTest.class.getResource("/ops/EGI-algorithm.json"));
	}

	@Test
	public void test() throws URISyntaxException, ParseException, IOException {

		// Use Operations Manager
		// Prepare Resource File
		URL resJsonFile = DTimelineTest.class.getResource("/ops/EGI-algorithm.json");
		File JsonFile = new File(resJsonFile.toURI());
		// Instatiate class
		OpsManager opsMgr = new OpsManager();
		// Test loading file
		opsMgr.loadJson(JsonFile);

		// Initialize Discrete Timeline for testing
		DTimeline dtl = new DTimeline();
		// Clear Samples
		dtl.clearSamples();
		// Assert default array size = 288
		// assertEquals("Default sample array size must be
		// 288",dtl.samples.length,288);
		// Assert each initialized element that is = -1
		for (int i = 0; i < dtl.samples.length; i++) {
			assertEquals("Init element must be -1", dtl.samples[i], -1);
		}
		// Assert Start state
		assertEquals("Start State", dtl.getStartState(), -1);
		// Set Start state
		dtl.setStartState(opsMgr.getIntStatus("CRITICAL"));
		// Assert Start state
		assertEquals("Start State", dtl.getStartState(), 4);
		// Create a sample timeline
		dtl.insert("2014-01-15T01:33:44Z", opsMgr.getIntStatus("OK"));
		dtl.insert("2014-01-15T05:53:40Z", opsMgr.getIntStatus("WARNING"));
		dtl.insert("2014-01-15T12:33:22Z", opsMgr.getIntStatus("UNKNOWN"));
		dtl.settle(opsMgr.getIntStatus("MISSING"));

		// Create expected state array
		int[] expected = new int[288];
		// start state = "CRITICAL"
		int expectedSS = opsMgr.getIntStatus("CRITICAL");
		// First state has timestamp 01:33:44
		// In seconds = 5624
		// In minutes = 5624 / 60 = 94
		// In slots = 94 / 5 = 19
		// So for i=0;i<19-1 fill with expectedSS
		for (int i = 0; i < 19 - 1; i++)
			expected[i] = expectedSS;

		// Second state has timestamp 05:53:40
		// In seconds = 21220
		// In minutes = 21220 / 60 = 354
		// In slots = 354 / 5 = 71
		// So for i=18;i<71-1 fill with first timestamp
		for (int i = 18; i < 71 - 1; i++)
			expected[i] = opsMgr.getIntStatus("OK");

		// Second state has timestamp 12:33:22
		// In seconds = 45202
		// In minutes = 45202 / 60 = 753
		// In slots = 753 / 5 = 151
		// So for i=71;i<151-1 fill with first timestamp
		for (int i = 70; i < 151 - 1; i++)
			expected[i] = opsMgr.getIntStatus("WARNING");

		for (int i = 150; i < expected.length; i++)
			expected[i] = opsMgr.getIntStatus("UNKNOWN");

		assertArrayEquals("Aggregation check", expected, dtl.samples);

		// New Timeline
		DTimeline dt2 = new DTimeline();
		dt2.setStartState(opsMgr.getIntStatus("OK"));

		dt2.insert("2015-01-24T00:35:21Z", opsMgr.getIntStatus("OK"));
		dt2.insert("2015-01-24T01:35:23Z", opsMgr.getIntStatus("OK"));
		dt2.insert("2015-01-24T02:35:22Z", opsMgr.getIntStatus("OK"));
		dt2.insert("2015-01-24T03:35:24Z", opsMgr.getIntStatus("OK"));
		dt2.insert("2015-01-24T04:35:22Z", opsMgr.getIntStatus("OK"));
		dt2.insert("2015-01-24T05:35:18Z", opsMgr.getIntStatus("OK"));
		dt2.insert("2015-01-24T06:35:23Z", opsMgr.getIntStatus("OK"));
		dt2.insert("2015-01-24T07:35:22Z", opsMgr.getIntStatus("OK"));
		dt2.insert("2015-01-24T08:35:18Z", opsMgr.getIntStatus("OK"));
		dt2.insert("2015-01-24T10:35:17Z", opsMgr.getIntStatus("OK"));
		dt2.insert("2015-01-24T11:35:24Z", opsMgr.getIntStatus("OK"));
		dt2.insert("2015-01-24T12:35:19Z", opsMgr.getIntStatus("OK"));
		dt2.insert("2015-01-24T13:35:23Z", opsMgr.getIntStatus("OK"));
		dt2.insert("2015-01-24T14:35:22Z", opsMgr.getIntStatus("OK"));
		dt2.insert("2015-01-24T15:35:22Z", opsMgr.getIntStatus("OK"));
		dt2.insert("2015-01-24T16:35:23Z", opsMgr.getIntStatus("OK"));
		dt2.insert("2015-01-24T17:35:22Z", opsMgr.getIntStatus("OK"));
		dt2.insert("2015-01-24T18:35:26Z", opsMgr.getIntStatus("OK"));
		dt2.insert("2015-01-24T19:35:27Z", opsMgr.getIntStatus("OK"));
		dt2.insert("2015-01-24T20:35:22Z", opsMgr.getIntStatus("OK"));
		dt2.insert("2015-01-24T21:35:26Z", opsMgr.getIntStatus("OK"));
		dt2.insert("2015-01-24T22:35:21Z", opsMgr.getIntStatus("OK"));
		dt2.insert("2015-01-24T22:45:51Z", opsMgr.getIntStatus("OK"));
		dt2.insert("2015-01-24T23:45:52Z", opsMgr.getIntStatus("OK"));

		dt2.settle(opsMgr.getIntStatus("MISSING"));

		int[] expected2 = new int[288];
		for (int i = 0; i < 288; i++)
			expected2[i] = 0;

		assertArrayEquals("Aggregation check", expected2, dt2.samples);

	}

}
