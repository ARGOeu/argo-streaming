package ops;


import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.ParseException;

import org.junit.Test;

public class CAggregatorTest {

	@Test
	public void testAggregation() throws ParseException, IOException, URISyntaxException  {
		
		// Prepare Resource File
		URL resJsonFile = CAggregatorTest.class.getResource("/ops/EGI-algorithm.json");
		File JsonFile = new File(resJsonFile.toURI());
		// Instantiate class
		OpsManager opsMgr = new OpsManager();
		// Test loading file
		opsMgr.loadJson(JsonFile);
		
        CAggregator agg = new CAggregator();
         
        agg.insert("hostFoo.lan","2015-05-01T00:00:00Z",opsMgr.getIntStatus("OK"));
        agg.insert("hostFoo.lan","2015-05-01T09:00:00Z",opsMgr.getIntStatus("OK"));
        agg.insert("hostFoo.lan","2015-05-01T12:00:00Z",opsMgr.getIntStatus("OK"));
        agg.insert("hostFoo.lan","2015-05-01T22:00:00Z",opsMgr.getIntStatus("OK"));
        
        agg.insert("hostBar.lan","2015-05-01T00:00:00Z",opsMgr.getIntStatus("OK"));
        agg.insert("hostBar.lan","2015-05-01T02:00:00Z",opsMgr.getIntStatus("WARNING"));
        agg.insert("hostBar.lan","2015-05-01T22:00:00Z",opsMgr.getIntStatus("OK"));
        agg.insert("hostBar.lan","2015-05-01T23:00:00Z",opsMgr.getIntStatus("MISSING"));
        agg.insert("hostBar.lan","2015-05-01T23:22:00Z",opsMgr.getIntStatus("OK"));
        
        agg.insert("hostCat.lan","2015-05-01T00:00:00Z",opsMgr.getIntStatus("OK"));
        agg.insert("hostCat.lan","2015-05-01T04:00:00Z",opsMgr.getIntStatus("CRITICAL"));
        agg.insert("hostCat.lan","2015-05-01T05:00:00Z",opsMgr.getIntStatus("CRITICAL"));
        agg.insert("hostCat.lan","2015-05-01T06:00:00Z",opsMgr.getIntStatus("OK"));
        agg.insert("hostCat.lan","2015-05-01T23:22:00Z",opsMgr.getIntStatus("OK"));
	
        agg.aggregate(opsMgr, "AND");
        
        CTimeline expected = new CTimeline("2015-05-01T00:00:00Z");
        expected.insert("2015-05-01T00:00:00Z",opsMgr.getIntStatus("OK"));
        expected.insert("2015-05-01T02:00:00Z",opsMgr.getIntStatus("WARNING"));
        expected.insert("2015-05-01T04:00:00Z",opsMgr.getIntStatus("CRITICAL"));
        expected.insert("2015-05-01T06:00:00Z",opsMgr.getIntStatus("WARNING"));
        expected.insert("2015-05-01T22:00:00Z",opsMgr.getIntStatus("OK"));
        expected.insert("2015-05-01T23:00:00Z",opsMgr.getIntStatus("MISSING"));
        expected.insert("2015-05-01T23:22:00Z",opsMgr.getIntStatus("OK"));
        
        assertEquals(expected.getSamples(),agg.getSamples());
        
	}
	
	@Test
	public void testSingleAggregation() throws ParseException, IOException, URISyntaxException  {
		
		// Prepare Resource File
		URL resJsonFile = CAggregatorTest.class.getResource("/ops/EGI-algorithm.json");
		File JsonFile = new File(resJsonFile.toURI());
		// Instantiate class
		OpsManager opsMgr = new OpsManager();
		// Test loading file
		opsMgr.loadJson(JsonFile);
		
        CAggregator agg = new CAggregator();
         
        agg.insert("hostFoo.lan","2015-05-01T00:00:00Z",opsMgr.getIntStatus("OK"));
        agg.insert("hostFoo.lan","2015-05-01T09:00:00Z",opsMgr.getIntStatus("OK"));
        agg.insert("hostFoo.lan","2015-05-01T12:00:00Z",opsMgr.getIntStatus("OK"));
        agg.insert("hostFoo.lan","2015-05-01T22:00:00Z",opsMgr.getIntStatus("OK"));
        
     
	
        agg.aggregate(opsMgr, "AND");
        
        CTimeline expected = new CTimeline("2015-05-01T00:00:00Z");
        expected.insert("2015-05-01T00:00:00Z",opsMgr.getIntStatus("OK"));
    
        assertEquals(expected.getSamples(),agg.getSamples());
        
	}
	
	
}