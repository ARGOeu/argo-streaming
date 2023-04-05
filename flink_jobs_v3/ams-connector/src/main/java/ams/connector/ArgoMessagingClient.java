package ams.connector;


import org.mortbay.log.Log;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.util.Base64;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple http client for pulling and acknowledging messages from AMS service
 * http API
 */
public class ArgoMessagingClient {

    static Logger LOG = LoggerFactory.getLogger(ArgoMessagingClient.class);
    // Http Client for contanting AMS service
    private CloseableHttpClient httpClient = null;
    // AMS endpoint (hostname:port or hostname)
    private String endpoint = null;
    // AMS project (/v1/projects/{project})
    private String project = null;
    // AMS token (?key={token})
    private String token = null;
    // AMS subscription (/v1/projects/{project}/subscriptions/{sub})
    private String sub = null;
    // protocol (https,http)
    private String proto = null;
    // numer of message to be pulled;
    private String maxMessages = "";
    // ssl verify or not
    private boolean verify = true;
    // proxy
    private URI proxy = null;
    private String topic;
    private String runDate;

    // Utility inner class for holding list of messages and acknowledgements
    private class MsgAck {

        String[] msgs;
        String[] ackIds;

        private MsgAck(String[] msgs, String[] ackIds) {
            this.msgs = msgs;
            this.ackIds = ackIds;
        }

    }

    public ArgoMessagingClient() {
        this.httpClient = HttpClients.createDefault();
        this.proto = "https";
        this.token = "token";
        this.endpoint = "localhost";
        this.project = "test_project";
        this.sub = "test_sub";
        this.maxMessages = "100";
        this.proxy = null;
        this.runDate = null;
    }

    public ArgoMessagingClient(String method, String token, String endpoint, String project, String sub, int batch,
                               boolean verify, String runDate) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {

        this.proto = method;
        this.token = token;
        this.endpoint = endpoint;
        this.project = project;
        this.sub = sub;
        this.maxMessages = String.valueOf(batch);
        this.verify = verify;

        this.httpClient = buildHttpClient();
        this.runDate = runDate;

    }

    public ArgoMessagingClient(String method, String token, String endpoint, String project, String topic,
                               boolean verify, String runDate) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {

        this.proto = method;
        this.token = token;
        this.endpoint = endpoint;
        this.project = project;
        this.topic = topic;
        this.verify = verify;
        this.runDate = runDate;
        this.httpClient = buildHttpClient();

    }

    /**
     * Initializes Http Client (if not initialized during constructor)
     *
     * @return
     */
    private CloseableHttpClient buildHttpClient()
            throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        if (this.verify) {
            return this.httpClient = HttpClients.createDefault();
        } else {
            return this.httpClient = HttpClients.custom().setSSLSocketFactory(selfSignedSSLF()).build();
        }
    }

    /**
     * Create an SSL Connection Socket Factory with a strategy to trust self
     * signed certificates
     */
    private SSLConnectionSocketFactory selfSignedSSLF()
            throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        SSLContextBuilder sslBuild = new SSLContextBuilder();
        sslBuild.loadTrustMaterial(null, new TrustSelfSignedStrategy());
        return new SSLConnectionSocketFactory(sslBuild.build(), NoopHostnameVerifier.INSTANCE);
    }

    /**
     * Set AMS http client to use http proxy
     */
    public void setProxy(String proxyURL) throws URISyntaxException {
        // parse proxy url
        this.proxy = URI.create(proxyURL);
    }

    /**
     * Set AMS http client to NOT use an http proxy
     */
    public void unsetProxy() {
        this.proxy = null;
    }

    /**
     * Create a configuration for using http proxy on each request
     */
    private RequestConfig createProxyCfg() {
        HttpHost proxy = new HttpHost(this.proxy.getHost(), this.proxy.getPort(), this.proxy.getScheme());
        RequestConfig config = RequestConfig.custom().setProxy(proxy).build();
        return config;
    }

    public void logIssue(CloseableHttpResponse resp) throws UnsupportedOperationException, IOException {
        InputStreamReader isRdr = new InputStreamReader(resp.getEntity().getContent());
        BufferedReader bRdr = new BufferedReader(isRdr);
        int statusCode = resp.getStatusLine().getStatusCode();

        // Parse error content from api response
        StringBuilder result = new StringBuilder();
        String rLine;
        while ((rLine = bRdr.readLine()) != null) {
            result.append(rLine);
        }
        isRdr.close();
        Log.warn("ApiStatusCode={}, ApiErrorMessage={}", String.valueOf(statusCode), result);

    }

    /**
     * Properly compose url for each AMS request
     */
    public String composeURL(String method) {

        switch (method) {
            case "publish":
                return proto + "://" + endpoint + "/v1/projects/" + project + "/topics/" + topic + ":" + method;
            case "offsets":
                return proto + "://" + endpoint + "/v1/projects/" + project + "/subscriptions/" + this.sub + ":" + method;
            case "timeToOffset":
                return proto + "://" + endpoint + "/v1/projects/" + project + "/subscriptions/" + this.sub + ":" + method+"?time=" + this.runDate;
            default:
                return proto + "://" + endpoint + "/v1/projects/" + project + "/subscriptions/" + sub + ":" + method;
        }

    }

    /**
     * Executes a pull request against AMS api
     */
    public MsgAck doPull() throws IOException, KeyManagementException, NoSuchAlgorithmException, KeyStoreException {

        ArrayList<String> msgList = new ArrayList<String>();
        ArrayList<String> ackIdList = new ArrayList<String>();

        // Create the http post to pull
        HttpPost postPull = new HttpPost(this.composeURL("pull"));
        String body = "{\"maxMessages\":\"" + this.maxMessages + "\",\"returnImmediately\":\"true\"}";

        postPull.addHeader("Accept", "application/json");
        postPull.addHeader("x-api-key", this.token);
        postPull.addHeader("Content-type", "application/json");

        StringEntity postBody = new StringEntity(body);
        postBody.setContentType("application/json");

        postPull.setEntity(postBody);

        if (this.httpClient == null) {
            this.httpClient = buildHttpClient();
        }

        // check for proxy
        if (this.proxy != null) {
            postPull.setConfig(createProxyCfg());
        }

        CloseableHttpResponse response = this.httpClient.execute(postPull);
        String msg = "";
        String ackId = "";
        StringBuilder result = new StringBuilder();

        HttpEntity entity = response.getEntity();

        int statusCode = response.getStatusLine().getStatusCode();

        if (entity != null && statusCode == 200) {

            InputStreamReader isRdr = new InputStreamReader(entity.getContent());
            BufferedReader bRdr = new BufferedReader(isRdr);

            String rLine;

            while ((rLine = bRdr.readLine()) != null) {
                result.append(rLine);
            }

            // Gather message from json
            JsonParser jsonParser = new JsonParser();
            // parse the json root object
            Log.info("response: {}", result.toString());
            JsonElement jRoot = jsonParser.parse(result.toString());

            JsonArray jRec = jRoot.getAsJsonObject().get("receivedMessages").getAsJsonArray();

            // if has elements
            for (JsonElement jMsgItem : jRec) {
                JsonElement jMsg = jMsgItem.getAsJsonObject().get("message");
                JsonElement jAckId = jMsgItem.getAsJsonObject().get("ackId");
                msg = jMsg.toString();
                ackId = jAckId.toString();
                msgList.add(msg);
                ackIdList.add(ackId);
            }

            isRdr.close();

        } else {

            logIssue(response);

        }

        response.close();

        String[] msgArr = msgList.toArray(new String[0]);
        String[] ackIdArr = ackIdList.toArray(new String[0]);

        // Return a Message array
        return new MsgAck(msgArr, ackIdArr);

    }

    /**
     * Executes a combination of Pull & Ack requests against AMS api
     */
    public String[] consume() throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
        String[] msgs = new String[0];
        // Try first to pull a message
        try {

            MsgAck msgAck = doPull();
            // get last ackid
            String ackId = "";
            if (msgAck.ackIds.length > 0) {
                ackId = msgAck.ackIds[msgAck.ackIds.length - 1];
            }

            if (ackId != "") {
                // Do an ack for the received message
                String ackRes = doAck(ackId);
                if (ackRes == "") {
                    Log.info("Message Acknowledged ackid:" + ackId);
                    msgs = msgAck.msgs;

                } else {
                    Log.warn("No acknowledment for ackid:" + ackId + "-" + ackRes);
                }
            }
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        return msgs;

    }

    /**
     * Executes an offsetByTimestamp request against AMS api
     */
    public int offsetByTimestamp() throws IOException, KeyManagementException, NoSuchAlgorithmException, KeyStoreException {


        String method = "timeToOffset";
        if (this.runDate == null) {
            method = "offsets";
        }
        HttpGet getOffset = new HttpGet((this.composeURL(method)));
        getOffset.addHeader("Accept", "application/json");
        getOffset.addHeader("x-api-key", this.token);
        getOffset.addHeader("Content-type", "application/json");

        if (this.httpClient == null) {
            this.httpClient = buildHttpClient();
        }

        // check for proxy
        if (this.proxy != null) {
            getOffset.setConfig(createProxyCfg());
        }

        CloseableHttpResponse response = this.httpClient.execute(getOffset);
        int offset = 0;
        StringBuilder result = new StringBuilder();

        HttpEntity entity = response.getEntity();

        int statusCode = response.getStatusLine().getStatusCode();

        if (entity != null && statusCode == 200) {

            InputStreamReader isRdr = new InputStreamReader(entity.getContent());
            BufferedReader bRdr = new BufferedReader(isRdr);

            String rLine;

            while ((rLine = bRdr.readLine()) != null) {
                result.append(rLine);
            }

            // Gather message from json
            JsonParser jsonParser = new JsonParser();
            // parse the json root object
            Log.info("response: {}", result.toString());
            JsonElement jRoot = jsonParser.parse(result.toString());

            if (this.runDate != null) {
                offset = jRoot.getAsJsonObject().get("offset").getAsInt();
            } else {
                offset = jRoot.getAsJsonObject().get("max").getAsInt();

            }
            // if has elements

            isRdr.close();

        } else {

            logIssue(response);

        }

        response.close();

        // Return a Message array
        return offset;

    }

    /**
     * Executes a get offset requests against AMS api
     */
    public int offset() throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
        // Try first to pull a message
        int offset = 0;
        try {

            offset = offsetByTimestamp();
            // get last ackid

            if (offset != 0) {
                // Do an ack for the received message
                Log.info("Current offset is :" + offset);

            } else {
                Log.warn("Not updated offset:");
            }

        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        return offset;

    }

    /**
     * Executes a modify offset request against AMS api
     * @offset , the offset number to modify
     */
    public int modifyOffset(int offset) throws IOException, KeyManagementException, NoSuchAlgorithmException, KeyStoreException {

        // Create the http post to pull
        String url = proto + "://" + endpoint + "/v1/projects/" + project + "/subscriptions/" + this.sub + ":" + "modifyOffset";

        HttpPost postModifyOffset = new HttpPost(url);

        postModifyOffset.addHeader("Accept", "application/json");
        postModifyOffset.addHeader("x-api-key", this.token);
        postModifyOffset.addHeader("Content-type", "application/json");
        System.out.println("modify offset is :" + offset);
        String body = "{\"offset\":" + offset + "}";

        StringEntity postBody = new StringEntity(body);
        postBody.setContentType("application/json");

        postModifyOffset.setEntity(postBody);

        if (this.httpClient == null) {
            this.httpClient = buildHttpClient();
        }

        // check for proxy
        if (this.proxy != null) {
            postModifyOffset.setConfig(createProxyCfg());
        }

        CloseableHttpResponse response = this.httpClient.execute(postModifyOffset);
        StringBuilder result = new StringBuilder();

        HttpEntity entity = response.getEntity();

        int statusCode = response.getStatusLine().getStatusCode();

        if (entity != null && statusCode == 200) {

            try ( InputStreamReader isRdr = new InputStreamReader(entity.getContent())) {
                BufferedReader bRdr = new BufferedReader(isRdr);

                String rLine;
                while ((rLine = bRdr.readLine()) != null) {
                    result.append(rLine);
                }
                isRdr.close();
                // Gather message from json
                // JsonParser jsonParser = new JsonParser();
                // parse the json root object
                Log.info("modify offset response: {}", result.toString());
            }
        } else {

            logIssue(response);

        }

        response.close();

        // Return a Message array
        return offset;

    }

    /**
     * Executes an Acknowledge request against AMS api
     */
    public String doAck(String ackId) throws IOException {

        // Create the http post to ack
        HttpPost postAck = new HttpPost(this.composeURL("acknowledge"));
        String body = "{\"ackIds\":[" + ackId + "]}";
        postAck.addHeader("Accept", "application/json");
        postAck.addHeader("x-api-key", this.token);
        postAck.addHeader("Content-type", "application/json");

        StringEntity postBody = new StringEntity(body);

        postBody.setContentType("application/json");
        postAck.setEntity(postBody);

        // check for proxy
        if (this.proxy != null) {
            postAck.setConfig(createProxyCfg());
        }

        CloseableHttpResponse response = httpClient.execute(postAck);
        String resMsg = "";
        StringBuilder result = new StringBuilder();

        HttpEntity entity = response.getEntity();
        int status = response.getStatusLine().getStatusCode();

        if (status != 200) {

            InputStreamReader isRdr = new InputStreamReader(entity.getContent());
            BufferedReader bRdr = new BufferedReader(isRdr);

            String rLine;

            while ((rLine = bRdr.readLine()) != null) {
                result.append(rLine);
            }

            resMsg = result.toString();
            isRdr.close();

        } else {
            // Log any api errors
            logIssue(response);
        }
        response.close();
        // Return a resposeMessage
        return resMsg;

    }

    /**
     * Executes a combination of Pull & Ack requests against AMS api
     */
    public void publish(String in) throws IOException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        // Create the http post to pull
        String encodedString = Base64.getEncoder().encodeToString(in.getBytes());

        HttpPost postPublish = new HttpPost(this.composeURL("publish"));
        String body = "{\"messages\": [ {\"data\":\"" + encodedString + "\"}]}";

        postPublish.addHeader("Accept", "application/json");
        postPublish.addHeader("x-api-key", this.token);
        postPublish.addHeader("Content-type", "application/json");

        StringEntity postBody = new StringEntity(body);

        postBody.setContentType("application/json");
        postPublish.setEntity(postBody);

        if (this.httpClient == null) {
            this.httpClient = buildHttpClient();
        }

        // check for proxy
        if (this.proxy != null) {
            postPublish.setConfig(createProxyCfg());
        }

        try ( CloseableHttpResponse response = this.httpClient.execute(postPublish)) {
            StringBuilder result = new StringBuilder();

            HttpEntity entity = response.getEntity();

            int statusCode = response.getStatusLine().getStatusCode();

            if (entity != null && statusCode == 200) {

                try ( InputStreamReader isRdr = new InputStreamReader(entity.getContent())) {
                    BufferedReader bRdr = new BufferedReader(isRdr);

                    String rLine;
                    while ((rLine = bRdr.readLine()) != null) {
                        result.append(rLine);
                    }
                    isRdr.close();
                    // Gather message from json
                    // JsonParser jsonParser = new JsonParser();
                    // parse the json root object
                    Log.info("publish response: {}", result.toString());
                }
            } else {
                logIssue(response);

            }
            response.close();
        }

    }

    /**
     * Close AMS http client
     */
    public void close() throws IOException {
        this.httpClient.close();
    }
}