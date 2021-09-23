package ops;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.ParseException;
import java.util.Map.Entry;

import org.joda.time.DateTime;
import org.junit.Test;

public class CTimelineTest {

	@Test
	public void constructorTest() {
		CTimeline t1 = new CTimeline();
		CTimeline t2 = new CTimeline("2015-05-01T00:05:00Z");
		CTimeline t3 = new CTimeline("2015-05-01T00:22:00Z",5);
		
		assertEquals(true,t1.isEmpty());
		assertEquals(null,t1.getDate());
		
		assertEquals(true,t2.isEmpty());
		assertEquals("2015-05-01",t2.getDate().toString());
		
		assertEquals(false,t3.isEmpty());
		assertEquals("2015-05-01",t3.getDate().toString());
		assertEquals(1,t3.getLength());
		
		Entry<DateTime,Integer>item = t3.getSamples().iterator().next();
		assertEquals("2015-05-01T00:00:00Z",item.getKey().toString("yyyy-MM-dd'T'HH:mm:ss'Z'"));
		assertEquals(new Integer(5),item.getValue());
	}
	
	@Test
	public void testGet() {
		
		CTimeline t1 = new CTimeline("2015-05-01T11:00:00Z",3);
		t1.insert("2015-05-01T11:00:00Z",1);
		t1.insert("2015-05-01T13:00:00Z",2);
		t1.insert("2015-05-01T22:00:00Z",6);
		
		assertEquals(4, t1.getLength());
		
		CTimeline t2 = new CTimeline("2015-05-01T11:00:00Z",3);
		t2.insert("2015-05-01T11:00:00Z",1);
		t2.insert("2015-05-01T13:00:00Z",2);
		t2.insert("2015-05-01T00:00:00Z",6);
		
		assertEquals(3, t2.getLength());
		
		// Test Get
		assertEquals(1,t1.get("2015-05-01T11:00:00Z"));
		assertEquals(1,t1.get("2015-05-01T11:05:00Z"));
		assertEquals(1,t1.get("2015-05-01T12:05:00Z"));
		assertEquals(2,t1.get("2015-05-01T13:05:00Z"));
		assertEquals(6,t1.get("2015-05-01T22:05:00Z"));
		assertEquals(3,t1.get("2015-05-01T10:05:00Z"));
		
		assertEquals(1,t2.get("2015-05-01T11:00:00Z"));
		assertEquals(1,t2.get("2015-05-01T11:05:00Z"));
		assertEquals(1,t2.get("2015-05-01T12:05:00Z"));
		assertEquals(2,t2.get("2015-05-01T13:05:00Z"));
		assertEquals(2,t2.get("2015-05-01T22:05:00Z"));
		assertEquals(6,t2.get("2015-05-01T10:05:00Z"));
	}
	
	@Test
	public void testBulkInserts() throws ParseException  {

		
		// Test optimization and insert and correct order
		CTimeline ctl = new CTimeline("2015-05-01T00:00:00Z");
		CTimeline ctl2 = new CTimeline("2015-05-01T00:00:00Z");
		CTimeline expCtl = new CTimeline("2015-05-01T00:00:00Z");
		
		
		ctl.insert("2015-05-01T11:00:00Z", 1);
		ctl.insert("2015-05-01T12:00:00Z", 1);
		ctl.insert("2015-05-01T13:00:00Z", 2);
		ctl.insert("2015-05-01T15:00:00Z", 2);
		ctl.insert("2015-05-01T16:00:00Z", 2);
		ctl.insert("2015-05-01T20:00:00Z", 2);
		ctl.insert("2015-05-01T22:00:00Z", 1);
		ctl.insert("2015-05-01T23:00:00Z", 1);
		
		ctl2.insert("2015-05-01T22:00:00Z", 1);
		ctl2.insert("2015-05-01T23:00:00Z", 1);
		ctl2.insert("2015-05-01T12:00:00Z", 1);
		ctl2.insert("2015-05-01T11:00:00Z", 1);
		ctl2.insert("2015-05-01T16:00:00Z", 2);
		ctl2.insert("2015-05-01T13:00:00Z", 2);
		ctl2.insert("2015-05-01T15:00:00Z", 2);
		ctl2.insert("2015-05-01T20:00:00Z", 2);

		// assert that correct order is retained through inserts
		assertEquals(ctl.getSamples(),ctl2.getSamples());
		
		// Check bulk insert
		CTimeline ctl3 = new CTimeline();
		ctl3.bulkInsert(ctl.getSamples());
		
		assertEquals(ctl2.getSamples(),ctl3.getSamples());

		// Check optimization in single element timeline
		CTimeline ctl4 = new CTimeline();
		ctl4.insert("2015-05-01T12:00:00Z",1);
		ctl.optimize();
		expCtl.clear();
		expCtl.insert("2015-05-01T12:00:00Z",1);
		
		assertEquals(ctl4.getSamples(),expCtl.getSamples());
	
	}
	
	@Test
	public void testOptimization() throws ParseException  {

		
		// Test optimization and insert and correct order
		CTimeline ctl = new CTimeline("2015-05-01T00:00:00Z");
		CTimeline expCtl = new CTimeline("2015-05-01T00:00:00Z");
		
		
		ctl.insert("2015-05-01T11:00:00Z", 1);
		ctl.insert("2015-05-01T12:00:00Z", 1);
		ctl.insert("2015-05-01T13:00:00Z", 2);
		ctl.insert("2015-05-01T15:00:00Z", 2);
		ctl.insert("2015-05-01T16:00:00Z", 2);
		ctl.insert("2015-05-01T20:00:00Z", 2);
		ctl.insert("2015-05-01T22:00:00Z", 1);
		ctl.insert("2015-05-01T23:00:00Z", 1);
		// Test optimization 
		expCtl.insert("2015-05-01T11:00:00Z",1);
		expCtl.insert("2015-05-01T13:00:00Z",2);
		expCtl.insert("2015-05-01T22:00:00Z",1);

		ctl.optimize();
		
		assertEquals(expCtl.getSamples(),ctl.getSamples());
		
	
	}
	
	@Test
	public void testAggregation() throws URISyntaxException, IOException {
		
		// Prepare Resource File
		URL resJsonFile = CTimelineTest.class.getResource("/ops/EGI-algorithm.json");
		File JsonFile = new File(resJsonFile.toURI());
		// Instantiate class
		OpsManager opsMgr = new OpsManager();
		// Test loading file
		opsMgr.loadJson(JsonFile);
		
		CTimeline t1 = new CTimeline("2015-05-01T00:00:00Z");
		CTimeline t2 = new CTimeline("2015-05-01T00:00:00Z");
		
		
		t1.insert("2015-05-01T00:00:00Z", opsMgr.getIntStatus("OK"));
		t1.insert("2015-05-01T09:00:00Z", opsMgr.getIntStatus("WARNING"));
		t1.insert("2015-05-01T12:00:00Z", opsMgr.getIntStatus("OK"));
		
		t2.insert("2015-05-01T00:00:00Z", opsMgr.getIntStatus("OK"));
		t2.insert("2015-05-01T22:00:00Z", opsMgr.getIntStatus("CRITICAL"));
		t2.insert("2015-05-01T22:23:00Z", opsMgr.getIntStatus("OK"));
		
		t1.aggregate(t2, opsMgr, opsMgr.getIntOperation("AND"));
		
		CTimeline expected = new CTimeline("2015-05-01T00:00:00Z");
		expected.insert("2015-05-01T00:00:00Z", opsMgr.getIntStatus("OK"));
		expected.insert("2015-05-01T09:00:00Z", opsMgr.getIntStatus("WARNING"));
		expected.insert("2015-05-01T12:00:00Z", opsMgr.getIntStatus("OK"));
		expected.insert("2015-05-01T22:00:00Z", opsMgr.getIntStatus("CRITICAL"));
		expected.insert("2015-05-01T22:23:00Z", opsMgr.getIntStatus("OK"));
		
		assertEquals(expected.getSamples(),t1.getSamples());
	}

}