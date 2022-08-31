package argo.streaming;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;

import java.io.FileReader;
import java.io.IOException;

import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Map;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.codec.binary.Base64;
import org.junit.Test;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import argo.avro.MetricProfile;

public class TestSyncDecoding {

	@Test
	public void test() throws URISyntaxException, IOException {
		URL resJsonFile = TestSyncDecoding.class.getResource("/request_metric_profile.json");
		File jsonFile = new File(resJsonFile.toURI());

		BufferedReader br = new BufferedReader(new FileReader(jsonFile));

		JsonParser jsonParser = new JsonParser();
		JsonElement jRoot = jsonParser.parse(br);

		JsonElement jMsg = jRoot.getAsJsonObject().get("messages").getAsJsonArray().get(0);

		String data = jMsg.getAsJsonObject().get("data").getAsString();
		// Decode from base64
		byte[] decoded64 = Base64.decodeBase64(data.getBytes("UTF-8"));
		JsonElement jAttr = jMsg.getAsJsonObject().get("attributes");

		Map<String, String> attr = SyncParse.parseAttributes(jAttr);

		assertEquals("check report attr", attr.get("report"), "ops-mon");
		assertEquals("check type attr", attr.get("type"), "metric_profile");
		assertEquals("check partition date attr", attr.get("partition_date"), "2017-11-12");

		URL resMetricProfile = TestSyncDecoding.class.getResource("/metric_profile.json");
		File jsonMetricProfile = new File(resMetricProfile.toURI());

		BufferedReader br2 = new BufferedReader(new FileReader(jsonMetricProfile));

		ArrayList<String> mpContents = new ArrayList<String>();

		String line = "";
		while ((line = br2.readLine()) != null && line.length() != 0) {
			mpContents.add(line);
		}

		// Check decoding of MetricProfile avro objects
		DatumReader<MetricProfile> avroReader = new SpecificDatumReader<MetricProfile>(MetricProfile.getClassSchema(),
				MetricProfile.getClassSchema(), new SpecificData());
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(decoded64, null);

		int i=0;
		while (!decoder.isEnd()) {
			MetricProfile cur = avroReader.read(null, decoder);
			assertEquals("check decoded avro records",cur.toString(),mpContents.get(i));
			i++;
		}

	}

}
