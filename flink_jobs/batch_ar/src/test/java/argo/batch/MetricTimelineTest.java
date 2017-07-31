package argo.batch;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import ops.DTimeline;
import ops.OpsManager;
import argo.avro.MetricData;

public class MetricTimelineTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", MetricTimelineTest.class.getResource("/ops/EGI-algorithm.json"));
	}

	@Test
	public void test() throws URISyntaxException, IOException, ParseException {
		// Load operations manager
		// Prepare Resource File
		URL resJsonFile = MetricTimelineTest.class.getResource("/ops/EGI-algorithm.json");
		File JsonFile = new File(resJsonFile.toURI());
		// Instatiate class
		OpsManager opsMgr = new OpsManager();
		// Test loading file
		opsMgr.loadJson(JsonFile);

		
		// Assert the state strings to integer mappings ("OK"=>0, "MISSING"=>3 etc)
		assertEquals(0,opsMgr.getIntStatus("OK"));
		assertEquals(1,opsMgr.getIntStatus("WARNING"));
		assertEquals(3,opsMgr.getIntStatus("MISSING"));
		assertEquals(4,opsMgr.getIntStatus("CRITICAL"));
		
		
		// List of MetricData
		ArrayList<MetricData> mdata = new ArrayList<MetricData>();
		mdata.add(new MetricData("2017-07-01T23:00:00Z", "CREAM-CE", "cream01.foo", "job_submit", "OK", "mon01.foo",
				"summary", "ok", null));
		mdata.add(new MetricData("2017-07-02T05:00:00Z", "CREAM-CE", "cream01.foo", "job_submit", "WARNING",
				"mon01.foo", "summary", "ok", null));
		mdata.add(new MetricData("2017-07-02T00:00:00Z", "CREAM-CE", "cream01.foo", "job_submit", "OK", "mon01.foo",
				"summary", "ok", null));
		mdata.add(new MetricData("2017-07-02T12:00:00Z", "CREAM-CE", "cream01.foo", "job_submit", "CRITICAL",
				"mon01.foo", "summary", "ok", null));
		mdata.add(new MetricData("2017-07-02T14:00:00Z", "CREAM-CE", "cream01.foo", "job_submit", "OK", "mon01.foo",
				"summary", "ok", null));

		DTimeline dtl = new DTimeline();
		boolean first = true;
		for (MetricData item : mdata) {
			if (first) {
				dtl.setStartState(opsMgr.getIntStatus(item.getStatus()));
				first =false;
			}
			
			dtl.insert(item.getTimestamp(), opsMgr.getIntStatus(item.getStatus()));
			
		}
		
		dtl.settle(opsMgr.getDefaultMissingInt());
		
		// Create expected metric timeline
		DTimeline etl = new DTimeline();
		
		for (int i=0;i<59;i++){
			etl.samples[i]=0;
		}
		
		for (int i=59;i<143;i++){
			etl.samples[i]=1;
		}
		
		for (int i=143;i<167;i++){
			etl.samples[i]=4;
		}
		
		for (int i=167;i<etl.samples.length;i++)
		{
			etl.samples[i]=0;
		}
	    
		assertArrayEquals("timeline creation check",etl.samples,dtl.samples);
	}

}
