package ops;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.ParseException;
import java.util.Arrays;

import org.junit.Test;

public class DIntergatorTest {

	@Test
	public void test() throws URISyntaxException, ParseException, IOException {

		URL resJsonFile = DIntergatorTest.class.getResource("/ops/EGI-algorithm.json");
		File JsonFile = new File(resJsonFile.toURI());

		OpsManager opsMgr = new OpsManager();
		// Test loading file
		opsMgr.loadJson(JsonFile);

		DTimeline dtl = new DTimeline();

		DIntegrator inter = new DIntegrator();

		dtl.setStartState(opsMgr.getIntStatus("OK"));
		dtl.insert("2015-01-24T20:21:01Z", opsMgr.getIntStatus("DOWNTIME"));
		dtl.insert("2015-01-24T20:39:21Z", opsMgr.getIntStatus("OK"));
		dtl.insert("2015-01-24T22:00:21Z", opsMgr.getIntStatus("CRITICAL"));
		dtl.insert("2015-01-24T22:42:21Z", opsMgr.getIntStatus("OK"));

		dtl.settle(opsMgr.getIntStatus("MISSING"));
		inter.calculateAR(dtl.samples, opsMgr);

		assertEquals(inter.availability, 95.83, 0.01);
		assertEquals(inter.reliability, 97.18, 0.01);

	}

}
