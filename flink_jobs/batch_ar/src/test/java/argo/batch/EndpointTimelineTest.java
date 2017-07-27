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

import argo.avro.MetricData;
import ops.DAggregator;
import ops.DTimeline;
import ops.OpsManager;

public class EndpointTimelineTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", EndpointTimelineTest.class.getResource("/ops/EGI-algorithm.json"));
	}

	@Test
	public void test() throws URISyntaxException, IOException, ParseException {
		// Load operations manager
		// Prepare Resource File
		URL resJsonFile = EndpointTimelineTest.class.getResource("/ops/EGI-algorithm.json");
		File JsonFile = new File(resJsonFile.toURI());
		// Instatiate class
		OpsManager opsMgr = new OpsManager();
		// Test loading file
		opsMgr.loadJson(JsonFile);

		// Assert the state strings to integer mappings ("OK"=>0, "MISSING"=>3
		// etc)
		assertEquals(0, opsMgr.getIntStatus("OK"));
		assertEquals(1, opsMgr.getIntStatus("WARNING"));
		assertEquals(3, opsMgr.getIntStatus("MISSING"));
		assertEquals(4, opsMgr.getIntStatus("CRITICAL"));

		// List of MetricData
		ArrayList<MetricData> mdata1 = new ArrayList<MetricData>();
		ArrayList<MetricData> mdata2 = new ArrayList<MetricData>();
		ArrayList<MetricData> mdata3 = new ArrayList<MetricData>();
		mdata1.add(new MetricData("2017-07-01T23:00:00Z", "CREAM-CE", "cream01.foo", "job_submit", "OK", "mon01.foo",
				"summary", "ok", null));
		mdata1.add(new MetricData("2017-07-02T05:00:00Z", "CREAM-CE", "cream01.foo", "job_submit", "WARNING",
				"mon01.foo", "summary", "ok", null));
		mdata1.add(new MetricData("2017-07-02T00:00:00Z", "CREAM-CE", "cream01.foo", "job_submit", "OK", "mon01.foo",
				"summary", "ok", null));
		mdata1.add(new MetricData("2017-07-02T12:00:00Z", "CREAM-CE", "cream01.foo", "job_submit", "CRITICAL",
				"mon01.foo", "summary", "ok", null));
		mdata1.add(new MetricData("2017-07-02T14:00:00Z", "CREAM-CE", "cream01.foo", "job_submit", "OK", "mon01.foo",
				"summary", "ok", null));
		mdata2.add(new MetricData("2017-07-01T23:00:00Z", "CREAM-CE", "cream01.foo", "job_cancel", "OK", "mon01.foo",
				"summary", "ok", null));
		mdata2.add(new MetricData("2017-07-02T16:00:00Z", "CREAM-CE", "cream01.foo", "job_cancel", "OK", "mon01.foo",
				"summary", "ok", null));
		mdata2.add(new MetricData("2017-07-02T19:00:00Z", "CREAM-CE", "cream01.foo", "job_cancel", "CRITICAL",
				"mon01.foo", "summary", "ok", null));
		mdata2.add(new MetricData("2017-07-02T20:00:00Z", "CREAM-CE", "cream01.foo", "job_cancel", "OK", "mon01.foo",
				"summary", "ok", null));
		mdata3.add(new MetricData("2017-07-01T21:00:00Z", "CREAM-CE", "cream01.foo", "cert", "OK", "mon01.foo",
				"summary", "ok", null));
		mdata3.add(new MetricData("2017-07-02T21:00:00Z", "CREAM-CE", "cream01.foo", "cert", "OK", "mon01.foo",
				"summary", "ok", null));
		mdata3.add(new MetricData("2017-07-02T22:00:00Z", "CREAM-CE", "cream01.foo", "cert", "WARNING", "mon01.foo",
				"summary", "ok", null));
		mdata3.add(new MetricData("2017-07-02T23:00:00Z", "CREAM-CE", "cream01.foo", "cert", "OK", "mon01.foo",
				"summary", "ok", null));

		// Create Frist Metric Timeline
		DTimeline dtl1 = new DTimeline();

		boolean first = true;
		for (MetricData item : mdata1) {
			if (first) {
				dtl1.setStartState(opsMgr.getIntStatus(item.getStatus()));
				first = false;
			}

			dtl1.insert(item.getTimestamp(), opsMgr.getIntStatus(item.getStatus()));

		}

		dtl1.settle(opsMgr.getDefaultMissingInt());
		MonTimeline mtl1 = new MonTimeline("SITEA", "CREAM-CE", "cream01.foo", "job_submit");
		mtl1.setTimeline(dtl1.samples);

		DTimeline dtl2 = new DTimeline();

		first = true;
		for (MetricData item : mdata2) {
			if (first) {
				dtl2.setStartState(opsMgr.getIntStatus(item.getStatus()));
				first = false;
			}

			dtl2.insert(item.getTimestamp(), opsMgr.getIntStatus(item.getStatus()));

		}

		dtl2.settle(opsMgr.getDefaultMissingInt());
		MonTimeline mtl2 = new MonTimeline("SITEA", "CREAM-CE", "cream01.foo", "job_cancel");
		mtl2.setTimeline(dtl2.samples);

		DTimeline dtl3 = new DTimeline();

		first = true;
		for (MetricData item : mdata3) {
			if (first) {
				dtl3.setStartState(opsMgr.getIntStatus(item.getStatus()));
				first = false;
			}

			dtl3.insert(item.getTimestamp(), opsMgr.getIntStatus(item.getStatus()));

		}

		dtl3.settle(opsMgr.getDefaultMissingInt());
		MonTimeline mtl3 = new MonTimeline("SITEA", "CREAM-CE", "cream01.foo", "cert");
		mtl3.setTimeline(dtl3.samples);

		// Create 'job_submit' expected metric timeline
		DTimeline etl1 = new DTimeline();

		for (int i = 0; i < 59; i++) {
			etl1.samples[i] = 0;
		}

		for (int i = 59; i < 143; i++) {
			etl1.samples[i] = 1;
		}

		for (int i = 143; i < 167; i++) {
			etl1.samples[i] = 4;
		}

		for (int i = 167; i < etl1.samples.length; i++) {
			etl1.samples[i] = 0;
		}

		// Create 'job_cancel' expected metric timeline
		DTimeline etl2 = new DTimeline();

		for (int i = 0; i < 227; i++) {
			etl2.samples[i] = 0;
		}

		for (int i = 227; i < 239; i++) {
			etl2.samples[i] = 4;
		}

		for (int i = 239; i < etl2.samples.length; i++) {
			etl2.samples[i] = 0;
		}

		// Create 'cert' expected metric timeline
		DTimeline etl3 = new DTimeline();

		for (int i = 0; i < 267; i++) {
			etl3.samples[i] = 0;
		}

		for (int i = 263; i < 275; i++) {
			etl3.samples[i] = 1;
		}

		for (int i = 275; i < etl3.samples.length; i++) {
			etl3.samples[i] = 0;
		}

		// Assert that MonTimelines are as expected
		assertEquals("mon timeline 1: endpoint group check", "SITEA", mtl1.getGroup());
		assertEquals("mon timeline 1: service check", "CREAM-CE", mtl1.getService());
		assertEquals("mon timeline 1: hostname check", "cream01.foo", mtl1.getHostname());
		assertEquals("mon timeline 1: metric check", "job_submit", mtl1.getMetric());
		assertArrayEquals("mon timeline 1 samples check", etl1.samples, mtl1.getTimeline());

		assertEquals("mon timeline 2: endpoint group check", "SITEA", mtl2.getGroup());
		assertEquals("mon timeline 2: service check", "CREAM-CE", mtl2.getService());
		assertEquals("mon timeline 2: hostname check", "cream01.foo", mtl2.getHostname());
		assertEquals("mon timeline 2: metric check", "job_cancel", mtl2.getMetric());
		assertArrayEquals("mon timeline 2 samples check", etl2.samples, mtl2.getTimeline());

		assertEquals("mon timeline 3: endpoint group check", "SITEA", mtl3.getGroup());
		assertEquals("mon timeline 3: service check", "CREAM-CE", mtl3.getService());
		assertEquals("mon timeline 3: hostname check", "cream01.foo", mtl3.getHostname());
		assertEquals("mon timeline 3: metric check", "cert", mtl3.getMetric());
		assertArrayEquals("mon timeline 3 samples check", etl3.samples, mtl3.getTimeline());

		// Endpoint Aggregation
		ArrayList<MonTimeline> monDS = new ArrayList<MonTimeline>();
		monDS.add(mtl1);
		monDS.add(mtl2);
		monDS.add(mtl3);

		DAggregator dAgg = new DAggregator();
		for (MonTimeline item : monDS) {

			DTimeline dtl = new DTimeline();
			dtl.samples = item.getTimeline();

			dtl.setStartState(dtl.samples[0]);
			// Insert timeline directly into aggragator's hashtable
		
			dAgg.timelines.put(item.getMetric(), dtl);
			

		}
		// dont use settleAll because timelines in the aggregator are already
		// directly settled from mon timeline objects
		dAgg.aggregate("AND", opsMgr);

		int[] expResult = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1,
				1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
				1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
				1, 1, 1, 1, 1, 1, 1, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

		MonTimeline endTl = new MonTimeline("SITEA", "CREAM-CE", "cream01.foo", "");
		endTl.setTimeline(dAgg.aggregation.samples);

		assertEquals("endpoint timeline: endpoint group check", "SITEA", endTl.getGroup());
		assertEquals("endpoint timeline: service check", "CREAM-CE", endTl.getService());
		assertEquals("endpoint timeline: hostname check", "cream01.foo", endTl.getHostname());
		assertEquals("endpoint timeline: metric check", "", endTl.getMetric());
		assertArrayEquals("Assert aggregated endpoint timeline",expResult,endTl.getTimeline());
		

	}

}
