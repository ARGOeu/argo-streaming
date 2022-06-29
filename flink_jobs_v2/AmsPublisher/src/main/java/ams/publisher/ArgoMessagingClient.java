package ams.publisher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import javax.ws.rs.core.Request;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;

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
    private String topic = null;
    // protocol (https,http)
    private String proto = null;
    // numer of message to be pulled;
    private String maxMessages = "";
    // ssl verify or not
    private boolean verify = true;
    // proxy
    private URI proxy = null;

    public ArgoMessagingClient() {
        this.httpClient = HttpClients.createDefault();
        this.proto = "https";
        this.token = "token";
        this.endpoint = "localhost";
        this.project = "test_project";
        this.topic = "test_topic";
        this.maxMessages = "100";
        this.proxy = null;
    }

    public ArgoMessagingClient(String method, String token, String endpoint, String project, String topic,
            boolean verify) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {

        this.proto = method;
        this.token = token;
        this.endpoint = endpoint;
        this.project = project;
        this.topic = topic;
        this.verify = verify;

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
        int statusCode;
        StringBuilder result;
        try ( InputStreamReader isRdr = new InputStreamReader(resp.getEntity().getContent())) {
            BufferedReader bRdr = new BufferedReader(isRdr);
            statusCode = resp.getStatusLine().getStatusCode();
            // Parse error content from api response
            result = new StringBuilder();
            String rLine;
            while ((rLine = bRdr.readLine()) != null) {
                result.append(rLine);
            }
        }
        Log.warn("ApiStatusCode={}, ApiErrorMessage={}", statusCode, result);

    }

    /**
     * Properly compose url for each AMS request
     */
    public String composeURL(String method) {
        return proto + "://" + endpoint + "/v1/projects/" + project + "/topics/" + topic + ":" + method;
    }

    /**
     * Executes a combination of Pull & Ack requests against AMS api
     */
    public void publish(String in) throws IOException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        String encodedString = Base64.getEncoder().encodeToString(in.getBytes());
        // Create the http post to pull
        HttpPost postPublish = new HttpPost(this.composeURL("publish"));
        String body = "{\"messages\": [ {\"data\":\"" + encodedString + "\"}]}";

        postPublish.addHeader("Accept", "application/json");
        postPublish.addHeader("x-api-key", this.token);
        postPublish.addHeader("Content-type", "application/json");

        StringEntity postBody = new StringEntity(body);
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
                    Log.info("response: {}", result.toString());
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
