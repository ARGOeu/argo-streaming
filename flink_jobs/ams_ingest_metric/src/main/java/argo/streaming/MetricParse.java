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
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;

import com.google.gson.JsonElement;

import argo.avro.MetricData;



/**
 * SyncParse is a utility class providing methods to parse specific connector data in avro format
 */
public class MetricParse {
	
	/**
	 * Parses a byte array and decodes avro MetricData objects
	 */
	public static ArrayList<MetricData> parseGroupEndpoint(byte[] avroBytes) throws IOException{
		
		ArrayList<MetricData> result = new ArrayList<MetricData>();
		
		DatumReader<MetricData> avroReader = new SpecificDatumReader<MetricData>(MetricData.getClassSchema(),MetricData.getClassSchema(),new SpecificData());
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avroBytes, null);
		
		while (!decoder.isEnd()){
			MetricData cur = avroReader.read(null, decoder);
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
