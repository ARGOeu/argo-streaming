/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.utils;

import com.google.common.net.HttpHeaders;
import java.io.IOException;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author cthermolia
 */
public class RequestManager {

    public static JSONObject getMetricProfileRequest(String baseUri, String uid, String key) throws IOException, ParseException {
        JSONObject jsonresult = null;
        String uri = baseUri + "/metric_profiles/" + uid;
        CloseableHttpClient httpClient = HttpClients.createDefault();
        try {

            HttpGet request = new HttpGet(uri);
            // add request headers
            request.addHeader("x-api-key", key);
            request.addHeader(HttpHeaders.ACCEPT, "application/json");

            CloseableHttpResponse response = httpClient.execute(request);
            try {

                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    // return it as a String
                    String result = EntityUtils.toString(entity);

                    JSONParser parser = new JSONParser();
                    jsonresult = (JSONObject) parser.parse(result);
                }

            } finally {
                response.close();

            }
        } finally {
            httpClient.close();

        }
        return jsonresult;

    }
    
    
    public static JSONObject getTopologyEndpointRequest(String baseUri, String key) throws IOException, ParseException {
        JSONObject jsonresult = null;
        String uri = baseUri + "/topology/endpoints";
        CloseableHttpClient httpClient = HttpClients.createDefault();
        try {

            HttpGet request = new HttpGet(uri);
            // add request headers
            request.addHeader("x-api-key", key);
            request.addHeader(HttpHeaders.ACCEPT, "application/json");

            CloseableHttpResponse response = httpClient.execute(request);
            try {

                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    // return it as a String
                    String result = EntityUtils.toString(entity);

                    JSONParser parser = new JSONParser();
                    jsonresult = (JSONObject) parser.parse(result);
                }

            } finally {
                response.close();

            }
        } finally {
            httpClient.close();

        }
        return jsonresult;

    }
    
    
      public static JSONObject getOperationProfileRequest(String baseUri, String key) throws IOException, ParseException {
        JSONObject jsonresult = null;
        String uri = baseUri + "/operations_profiles";
        CloseableHttpClient httpClient = HttpClients.createDefault();
        try {

            HttpGet request = new HttpGet(uri);
            // add request headers
            request.addHeader("x-api-key", key);
            request.addHeader(HttpHeaders.ACCEPT, "application/json");

            CloseableHttpResponse response = httpClient.execute(request);
            try {

                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    // return it as a String
                    String result = EntityUtils.toString(entity);

                    JSONParser parser = new JSONParser();
                    jsonresult = (JSONObject) parser.parse(result);
                }

            } finally {
                response.close();

            }
        } finally {
            httpClient.close();

        }
        return jsonresult;

    }
  
}
