package argo.streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import com.google.gson.JsonElement;

import argo.avro.GroupEndpoint;
import argo.avro.MetricProfile;


/**
 * SyncParse is a utility class providing methods to parse specific connector data in avro format
 */
public class SyncParse {
	
	/**
	 * Parses a byte arrray and decodes avro GroupEndpoint objects
	 */
	public static ArrayList<GroupEndpoint> parseGroupEndpoint(byte[] avroBytes) throws IOException{
		
		ArrayList<GroupEndpoint> result = new ArrayList<GroupEndpoint>();
		
		DatumReader<GroupEndpoint> avroReader = new SpecificDatumReader<GroupEndpoint>(GroupEndpoint.getClassSchema());
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avroBytes, null);
		
		while (!decoder.isEnd()){
			GroupEndpoint cur = avroReader.read(null, decoder);
			result.add(cur);
		}
		
		return result;
	}
	
	/**
	 * Parses a byte arrray and decodes avro MetricProfile objects
	 */
	public static ArrayList<MetricProfile> parseMetricProfile(byte[] avroBytes) throws IOException{
		
		ArrayList<MetricProfile> result = new ArrayList<MetricProfile>();
		
		DatumReader<MetricProfile> avroReader = new SpecificDatumReader<MetricProfile>(MetricProfile.getClassSchema());
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avroBytes, null);
		
		while (!decoder.isEnd()){
			MetricProfile cur = avroReader.read(null, decoder);
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
	

}
