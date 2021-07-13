package sync;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import org.junit.BeforeClass;
import org.junit.Test;

public class WeightManagerTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", WeightManagerTest.class.getResource("/avro/weights_v2.avro"));
	}

	@Test
	public void test() throws IOException, URISyntaxException {
		// Prepare Resource File
		URL resAvroFile = WeightManagerTest.class.getResource("/avro/weights_v2.avro");
		File avroFile = new File(resAvroFile.toURI());
		// Instatiate class
		WeightManager wg = new WeightManager();
		// Test loading file
		wg.loadAvro(avroFile);
		assertNotNull("File Loaded", wg);

		// Test factor retrieval for various sites
		assertEquals("Factor for Taiwan-LCG2", wg.getWeight("hepspec", "Taiwan-LCG2"), 125913);
		assertEquals("Factor for UNI-DORTMUND", wg.getWeight("hepspec", "UNI-DORTMUND"), 16000);
		assertEquals("Factor for INFN-COSENZA", wg.getWeight("hepspec", "INFN-COSENZA"), 2006);
		assertEquals("Factor for CA-ALBERTA-WESTGRID-T2", wg.getWeight("hepspec", "CA-ALBERTA-WESTGRID-T2"), 9720);
		assertEquals("Factor for INFN-COSENZA", wg.getWeight("hepspec", "INFN-COSENZA"), 2006);
		assertEquals("Factor for T2_Estonia", wg.getWeight("hepspec", "T2_Estonia"), 48983);
		assertEquals("Factor for AEGIS09-FTN-KM", wg.getWeight("hepspec", "AEGIS09-FTN-KM"), 28);
		assertEquals("Factor for UNIBE-ID", wg.getWeight("hepspec", "UNIBE-ID"), 6278);
		assertEquals("Factor for NWICG_NDCMS", wg.getWeight("hepspec", "NWICG_NDCMS"), 0);
		assertEquals("Factor for CREATIS-INSA-LYON", wg.getWeight("hepspec", "CREATIS-INSA-LYON"), 720);
		assertEquals("Factor for FNAL_GPGRID_1", wg.getWeight("hepspec", "FNAL_GPGRID_1"), 344576);
		assertEquals("Factor for UA-MHI", wg.getWeight("hepspec", "UA-MHI"), 8984);
		assertEquals("Factor for SARA-MATRIX", wg.getWeight("hepspec", "SARA-MATRIX"), 139866);
		assertEquals("Factor for UKI-SCOTGRID-GLASGOW", wg.getWeight("hepspec", "UKI-SCOTGRID-GLASGOW"), 43878);
		assertEquals("Factor for IN2P3-LPSC", wg.getWeight("hepspec", "IN2P3-LPSC"), 11207);
		assertEquals("Factor for INFN-LECCE", wg.getWeight("hepspec", "INFN-LECCE"), 19);
		assertEquals("Factor for RUG-CIT", wg.getWeight("hepspec", "RUG-CIT"), 8988);
		assertEquals("Factor for GR-07-UOI-HEPLAB", wg.getWeight("hepspec", "GR-07-UOI-HEPLAB"), 1872);

		// Test zero factor return for unlisted sites
		assertEquals("Factor for FOO", wg.getWeight("hepspec", "FOO"), 0);
		assertEquals("Factor for BAR", wg.getWeight("hepspec", "BAR"), 0);

	}

}
