/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class RequestManager {

    public static JSONObject request(String uri, String key, String proxy) throws ParseException {
        JSONObject jsonresult = null;

        Request request = Request.Get(uri);
        // add request headers
        request.addHeader("x-api-key", key);
        request.addHeader("Accept", "application/json");
        if (proxy != null) {
            request = request.viaProxy(proxy);
        }
        String content = "{}";
        try {
            CloseableHttpClient httpClient = HttpClients.custom().build();
            Executor executor = Executor.newInstance(httpClient);
            content = executor.execute(request).returnContent().asString();

            JSONParser parser = new JSONParser();
            jsonresult = (JSONObject) parser.parse(content);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return jsonresult;
    }

    public static JsonElement callRequest(String uri, String key, String proxy) throws ParseException {
        JsonElement j_element = null;

        Request request = Request.Get(uri);
        // add request headers
        request.addHeader("x-api-key", key);
        request.addHeader("Accept", "application/json");
        if (proxy != null) {
            request = request.viaProxy(proxy);
        }
        String content = "{}";
        try {
            CloseableHttpClient httpClient = HttpClients.custom().build();
            Executor executor = Executor.newInstance(httpClient);
            content = executor.execute(request).returnContent().asString();

            JsonParser parser = new JsonParser();
            j_element = (JsonElement) parser.parse(content);

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return j_element;
    }

    public static JsonElement operationsProfileRequest(String apiUri, String operationsId, String key, String proxy, String dateStr) throws IOException, ParseException {

        String uri = apiUri + "/operations_profiles";
        if (dateStr == null) {
            uri = uri + operationsId;
        } else {
            uri = uri + "?date=" + dateStr;
        }
        return loadProfile(uri, key, proxy).get(0);
    }

//    public static JsonElement loadOperationProfile(String uri, String key, String proxy) throws IOException, org.json.simple.parser.ParseException {
//        JsonElement jsonElement = RequestManager.callRequest(uri, key, proxy);
//        JsonObject jsonObj=jsonElement.getAsJsonObject();
//        JsonArray dataObj = jsonObj.getAsJsonArray("data");
//        JsonElement dataElement=dataObj.get(0);
//      
//         
//        return dataElement;
//    }
    public static JsonElement metricProfileRequest(String apiUri, String metricId, String key, String proxy, String dateStr) throws IOException, ParseException {

        String uri = apiUri + "/metric_profiles" + "/" + metricId;
        if (dateStr != null) {
            uri = uri + "?date=" + dateStr;
        }
        return loadProfile(uri, key, proxy).get(0);

    }

    public static JsonElement aggregationProfileRequest(String apiUri, String aggregationId, String key, String proxy, String dateStr) throws IOException, ParseException {

        String uri = apiUri + "/aggregation_profiles" + "/" + aggregationId;
        if (dateStr != null) {
            uri = uri + "?date=" + dateStr;
        }
        return loadProfile(uri, key, proxy).get(0);

    }




    public static JsonArray endpointGroupProfileRequest(String apiUri, String key, String proxy, String reportname, String dateStr) throws IOException, ParseException {

        String uri = apiUri + "/topology/endpoints/by_report" + "/" + reportname;
        if (dateStr != null) {
            uri = uri + "?date=" + dateStr;
        }
        return loadProfile(uri, key, proxy);

    }

    public static JsonArray groupGroupProfileRequest(String apiUri, String key, String proxy, String reportname, String dateStr) throws IOException, ParseException {

        String uri = apiUri + "/topology/groups/by_report" + "/" + reportname;
        if (dateStr != null) {
            uri = uri + "?date=" + dateStr;
        }
        return loadProfile(uri, key, proxy);

    }

    public static JsonElement reportProfileRequest(String apiUri, String key, String proxy, String reportId) throws IOException, ParseException {

        //   String uri = apiUri + "/reports/";
//        if (dateStr != null) {
//            uri = uri + "?date=" + dateStr;
//        }
        String uri = apiUri + "/reports/" + reportId;
        return loadProfile(uri, key, proxy).get(0);

    }


    public static JsonArray loadProfile(String uri, String key, String proxy) throws IOException, org.json.simple.parser.ParseException {
        JsonElement jsonElement = RequestManager.callRequest(uri, key, proxy);
        JsonObject jsonObj = jsonElement.getAsJsonObject();
        JsonArray dataObj = jsonObj.getAsJsonArray("data");

        //JsonElement dataElement = dataObj.get(0);

        return dataObj;
    }

}