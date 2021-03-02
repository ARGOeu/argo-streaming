/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.utils;

import com.google.common.net.HttpHeaders;
import java.io.IOException;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author cthermolia
 */
public class RequestManager {
 public static JSONObject getAggregationProfileRequest(String baseUri, String key, String proxy) throws IOException, ParseException {
        JSONObject jsonresult = null;
        String uri = baseUri + "/aggregation_profiles";
        Request request = Request.Get(uri);
        // add request headers
        request.addHeader("x-api-key", key);
        request.addHeader(HttpHeaders.ACCEPT, "application/json");
        if (proxy!=null) {
            request = request.viaProxy(proxy);
        }
        String content = "{}";
        try {
            CloseableHttpClient httpClient = HttpClients.custom().build();
            Executor executor = Executor.newInstance(httpClient);
            content = executor.execute(request).returnContent().asString();

            JSONParser parser = new JSONParser();
            jsonresult = (JSONObject) parser.parse(content);

//
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return jsonresult;

    }

    public static JSONObject getMetricProfileRequest(String baseUri, String uid, String key, String proxy) throws IOException, ParseException {
        JSONObject jsonresult = null;
        String uri = baseUri + "/metric_profiles/" + uid;
        Request request = Request.Get(uri);
        // add request headers
        request.addHeader("x-api-key", key);
        request.addHeader(HttpHeaders.ACCEPT, "application/json");
        if (proxy!=null) {
            request = request.viaProxy(proxy);
        }
        String content = "{}";
        try {
            CloseableHttpClient httpClient = HttpClients.custom().build();
            Executor executor = Executor.newInstance(httpClient);
            content = executor.execute(request).returnContent().asString();

            JSONParser parser = new JSONParser();
            jsonresult = (JSONObject) parser.parse(content);

//
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return jsonresult;

    }

    public static JSONObject getTopologyEndpointRequest(String baseUri, String key, String proxy) throws IOException, ParseException {
        JSONObject jsonresult = null;
        String uri = baseUri + "/topology/endpoints";
        Request request = Request.Get(uri);
        // add request headers
        request.addHeader("x-api-key", key);
        request.addHeader(HttpHeaders.ACCEPT, "application/json");
        if (proxy!=null) {
            request = request.viaProxy(proxy);
        }
        String content = "{}";
        try {
            CloseableHttpClient httpClient = HttpClients.custom().build();
            Executor executor = Executor.newInstance(httpClient);
            content = executor.execute(request).returnContent().asString();

            JSONParser parser = new JSONParser();
            jsonresult = (JSONObject) parser.parse(content);

//
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return jsonresult;

    }

    public static JSONObject getOperationProfileRequest(String baseUri, String key, String proxy) throws IOException, ParseException {
        JSONObject jsonresult = null;
        String uri = baseUri + "/operations_profiles";
        Request request = Request.Get(uri);
        // add request headers
        request.addHeader("x-api-key", key);
        request.addHeader(HttpHeaders.ACCEPT, "application/json");
        if (proxy!=null) {
            request = request.viaProxy(proxy);
        }
        String content = "{}";
        try {
            CloseableHttpClient httpClient = HttpClients.custom().build();
            Executor executor = Executor.newInstance(httpClient);
            content = executor.execute(request).returnContent().asString();

            JSONParser parser = new JSONParser();
            jsonresult = (JSONObject) parser.parse(content);

//
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
return jsonresult;


}
}