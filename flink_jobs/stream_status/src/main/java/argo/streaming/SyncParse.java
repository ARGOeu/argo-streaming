package argo.streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import argo.amr.ApiResource;
import argo.avro.Downtime;
import argo.avro.GroupEndpoint;
import argo.avro.MetricProfile;


/**
 * SyncParse is a utility class providing methods to parse specific connector data in avro format
 */
public class SyncParse {
	
	/**
	 * Parses a byte array and decodes avro GroupEndpoint objects
	 */
	public static ArrayList<GroupEndpoint> parseGroupEndpoint(byte[] avroBytes) throws IOException{
		
		ArrayList<GroupEndpoint> result = new ArrayList<GroupEndpoint>();
		
		DatumReader<GroupEndpoint> avroReader = new SpecificDatumReader<GroupEndpoint>(GroupEndpoint.getClassSchema(),GroupEndpoint.getClassSchema(),new SpecificData());
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avroBytes, null);
		
		while (!decoder.isEnd()){
			GroupEndpoint cur = avroReader.read(null, decoder);
			result.add(cur);
		}
		
		return result;
	}
	
	/**
	 * Parses a byte array and decodes avro MetricProfile objects
	 */
	public static ArrayList<MetricProfile> parseMetricProfile(byte[] avroBytes) throws IOException{
		
		ArrayList<MetricProfile> result = new ArrayList<MetricProfile>();
		
		DatumReader<MetricProfile> avroReader = new SpecificDatumReader<MetricProfile>(MetricProfile.getClassSchema(),MetricProfile.getClassSchema(),new SpecificData());
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avroBytes, null);
		
		while (!decoder.isEnd()){
			MetricProfile cur = avroReader.read(null, decoder);
			result.add(cur);
		}
		
		return result;
	}
	
	/**
	 * Parses a byte array and decodes avro Downtime objects
	 */
	public static ArrayList<Downtime> parseDowntimes(byte[] avroBytes) throws IOException{
		
		ArrayList<Downtime> result = new ArrayList<Downtime>();
		
		DatumReader<Downtime> avroReader = new SpecificDatumReader<Downtime>(Downtime.getClassSchema(),Downtime.getClassSchema(),new SpecificData());
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avroBytes, null);
		
		while (!decoder.isEnd()){
			Downtime cur = avroReader.read(null, decoder);
			result.add(cur);
		}
		
		return result;
	}
	
	/**
	 * Parses attributes from a json attribute element
	 */
	public static Map<String,String> parseAttributes(JsonElement jAttr) throws IOException{
		
		Map<String,String> result = new HashMap<String,String>();
		if (jAttr!=null){
			Set<Entry<String, JsonElement>> jItems = jAttr.getAsJsonObject().entrySet();
			
			for (Entry<String, JsonElement> jItem : jItems){
				result.put(jItem.getKey(), jItem.getValue().getAsString());
			}
		}
		
		return result;
	}
	
	public static ArrayList<MetricProfile> parseMetricJSON(String content) {
		ArrayList<MetricProfile> results = new ArrayList<MetricProfile>();

		
		JsonParser jsonParser = new JsonParser();
		JsonElement jElement = jsonParser.parse(content);
		JsonObject jRoot = jElement.getAsJsonObject();
		String profileName = jRoot.get("name").getAsString();
		JsonArray jElements = jRoot.get("services").getAsJsonArray();
		for (int i = 0; i < jElements.size(); i++) {
			JsonObject jItem= jElements.get(i).getAsJsonObject();
			String service = jItem.get("service").getAsString();
			JsonArray jMetrics = jItem.get("metrics").getAsJsonArray();
			for (int j=0; j < jMetrics.size(); j++) {
				String metric = jMetrics.get(j).getAsString();
				
				Map<String,String> tags = new HashMap<String,String>();
				MetricProfile mp = new MetricProfile(profileName,service,metric,tags);
				results.add(mp);
			}
			
		}
		
		return results;
	}
	
	public static ArrayList<Downtime> parseDowntimesJSON (String content) {
		
		ArrayList<Downtime> results = new ArrayList<Downtime>();
		
		JsonParser jsonParser = new JsonParser();
		JsonElement jElement = jsonParser.parse(content);
		JsonObject jRoot = jElement.getAsJsonObject();
		JsonArray jElements = jRoot.get("endpoints").getAsJsonArray();
		for (int i = 0; i < jElements.size(); i++) {
			JsonObject jItem= jElements.get(i).getAsJsonObject();
			String hostname = jItem.get("hostname").getAsString();
			String service = jItem.get("service").getAsString();
			String startTime = jItem.get("start_time").getAsString();
			String endTime = jItem.get("end_time").getAsString();
			
			Downtime d = new Downtime(hostname,service,startTime,endTime);
			results.add(d);
		}
		
		return results;
		
	}
	
	public static ArrayList<GroupEndpoint> parseGroupEndpointJSON (String content) {
		
		ArrayList<GroupEndpoint> results = new ArrayList<GroupEndpoint>();
		
		JsonParser jsonParser = new JsonParser();
		JsonElement jElement = jsonParser.parse(content);
		JsonArray jRoot = jElement.getAsJsonArray();
		for (int i = 0; i < jRoot.size(); i++) {
			JsonObject jItem= jRoot.get(i).getAsJsonObject();
			String group = jItem.get("group").getAsString();
			String gType = jItem.get("type").getAsString();
			String service = jItem.get("service").getAsString();
			String hostname = jItem.get("hostname").getAsString();
			JsonObject jTags = jItem.get("tags").getAsJsonObject();
			Map<String,String> tags = new HashMap<String,String>();
		    for ( Entry<String, JsonElement> kv : jTags.entrySet()) {
		    	tags.put(kv.getKey(), kv.getValue().getAsString());
		    }
			GroupEndpoint ge = new GroupEndpoint(gType,group,service,hostname,tags);
			results.add(ge);
		}
		
		return results;
		
	}
	
	
	

}
