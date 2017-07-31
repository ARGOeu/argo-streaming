package sync;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;

import org.junit.BeforeClass;
import org.junit.Test;

import junitx.framework.ListAssert;

public class MetricProfilesTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", MetricProfilesTest.class.getResource("/avro/poem_sync_v2.avro"));
	}

	@Test
	public void test() throws URISyntaxException, IOException {
		// Prepare Resource File
		URL resAvroFile = MetricProfilesTest.class.getResource("/avro/poem_sync_v2.avro");
		File avroFile = new File(resAvroFile.toURI());
		// Instatiate class
		MetricProfiles mp = new MetricProfiles();
		// Test loading file
		mp.loadAvro(avroFile);
		assertNotNull("File Loaded", mp);

		// Test Loaded Metric Profile
		assertEquals("Only one metric profile must be loaded", mp.getProfiles().size(), 1);
		assertEquals("Profile ch.cern.sam.roc_critical must be loaded", mp.getProfiles().get(0).toString(),
				"ch.cern.sam.ROC_CRITICAL");

		// Test Loaded Metric Profile Services
		ArrayList<String> serviceList = new ArrayList<String>();
		serviceList.add("GRAM5");
		serviceList.add("QCG.Computing");
		serviceList.add("ARC-CE");
		serviceList.add("unicore6.TargetSystemFactory");
		serviceList.add("Site-BDII");
		serviceList.add("CREAM-CE");
		serviceList.add("SRMv2");

		ListAssert.assertEquals("Test Presence of Loaded Profile Services", mp.getProfileServices("ch.cern.sam.ROC_CRITICAL"),
				serviceList);

		// Test Loaded Metric Profile service metrics;

		// GRAM5 service
		ArrayList<String> gram5Metrics = new ArrayList<String>();
		gram5Metrics.add("hr.srce.GRAM-Auth");
		gram5Metrics.add("hr.srce.GRAM-CertLifetime");
		gram5Metrics.add("hr.srce.GRAM-Command");

		ListAssert.assertEquals("Test GRAM5 metrics", mp.getProfileServiceMetrics("ch.cern.sam.ROC_CRITICAL", "GRAM5"),
				gram5Metrics);
				// Test Loaded Metric Profile service metrics;

		// QCG service
		ArrayList<String> qcgMetrics = new ArrayList<String>();
		qcgMetrics.add("hr.srce.QCG-Computing-CertLifetime");
		qcgMetrics.add("pl.plgrid.QCG-Computing");

		ListAssert.assertEquals("Test QCG metrics", mp.getProfileServiceMetrics("ch.cern.sam.ROC_CRITICAL", "QCG.Computing"),
				qcgMetrics);

		// Site-BDII
		ArrayList<String> siteBdiiMetrics = new ArrayList<String>();
		siteBdiiMetrics.add("org.bdii.Entries");
		siteBdiiMetrics.add("org.bdii.Freshness");
		ListAssert.assertEquals("Test Site-BDII metrics", mp.getProfileServiceMetrics("ch.cern.sam.ROC_CRITICAL", "Site-BDII"),
				siteBdiiMetrics);

		// SRMv2
		ArrayList<String> srmv2metrics = new ArrayList<String>();
		srmv2metrics.add("hr.srce.SRM2-CertLifetime");
		srmv2metrics.add("org.sam.SRM-Del");
		srmv2metrics.add("org.sam.SRM-Get");
		srmv2metrics.add("org.sam.SRM-GetSURLs");
		srmv2metrics.add("org.sam.SRM-GetTURLs");
		srmv2metrics.add("org.sam.SRM-Ls");
		srmv2metrics.add("org.sam.SRM-LsDir");
		srmv2metrics.add("org.sam.SRM-Put");
		ListAssert.assertEquals("SRMv2 ", (mp.getProfileServiceMetrics("ch.cern.sam.ROC_CRITICAL", "SRMv2")), srmv2metrics);

		// Check Existense of Profile Service Metric

		assertTrue("Existence of CREAM-CE Metric",
				mp.checkProfileServiceMetric("ch.cern.sam.ROC_CRITICAL", "CREAM-CE", "emi.cream.CREAMCE-JobSubmit"));
		assertTrue("Existence of CREAM-CE Metric",
				mp.checkProfileServiceMetric("ch.cern.sam.ROC_CRITICAL", "CREAM-CE", "emi.cream.CREAMCE-JobSubmit"));
		assertTrue("Existence of CREAM-CE Metric",
				mp.checkProfileServiceMetric("ch.cern.sam.ROC_CRITICAL", "CREAM-CE", "emi.wn.WN-Bi"));
		assertTrue("Existence of CREAM-CE Metric",
				mp.checkProfileServiceMetric("ch.cern.sam.ROC_CRITICAL", "CREAM-CE", "emi.wn.WN-Csh"));
		assertTrue("Existence of CREAM-CE Metric",
				mp.checkProfileServiceMetric("ch.cern.sam.ROC_CRITICAL", "CREAM-CE", "emi.wn.WN-SoftVer"));
		assertTrue("Existence of CREAM-CE Metric",
				mp.checkProfileServiceMetric("ch.cern.sam.ROC_CRITICAL", "CREAM-CE", "hr.srce.CADist-Check"));
		assertTrue("Existence of CREAM-CE Metric",
				mp.checkProfileServiceMetric("ch.cern.sam.ROC_CRITICAL", "CREAM-CE", "hr.srce.CREAMCE-CertLifetime"));
		// False results
		assertTrue("ARC-CE doesn't have certLifetime",
				!(mp.checkProfileServiceMetric("ch.cern.sam.ROC_CRITICAL", "ARC-CE", "hr.srce.CREAMCE-CertLifetime")));
	}

}
