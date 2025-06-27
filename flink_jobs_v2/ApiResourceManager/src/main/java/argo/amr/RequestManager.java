/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.amr;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.logging.Log;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;

/**
 *
 * Establish a connection to the given url and request data
 */
public class RequestManager {

    private String proxy;
    private String token;
    private int timeoutSec;
    private boolean verify;
    static Logger LOG = LoggerFactory.getLogger(RequestManager.class);

    public RequestManager(String proxy, String token, int timeoutSec, boolean verify) {
        this.proxy = proxy;
        this.token = token;
        this.timeoutSec = timeoutSec;
        this.verify = verify;
    }

    public RequestManager(String proxy, String token) {
        this.proxy = proxy;
        this.token = token;
        this.timeoutSec = 30;
        this.verify = true;
    }

    /**
     * Contacts remote argo-web-api based on the full url of a resource its
     * content (expected in json format)
     *
     * @param fullURL String containing the full url representation of the
     * argo-web-api resource
     * @return A string representation of the resource json content
     * @throws ClientProtocolException
     * @throws IOException
     * @throws KeyStoreException
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     */

    public String getResource(String fullURL) throws UnknownHostException {

        Request r = Request.Get(fullURL)
                .addHeader("Accept", "application/json")
                .addHeader("Content-type", "application/json")
                .addHeader("x-api-key", this.token);

        if (!this.proxy.isEmpty()) {
            r = r.viaProxy(proxy);
        }

        r = r.connectTimeout(this.timeoutSec * 1000).socketTimeout(this.timeoutSec * 1000);

        String content = "{}";

        try {
            if (this.verify == false) {
                CloseableHttpClient httpClient = HttpClients.custom().setSSLSocketFactory(selfSignedSSLF()).build();
                Executor executor = Executor.newInstance(httpClient);
                content = executor.execute(r).returnContent().asString();
                httpClient.close();
            } else {
                content = r.execute().returnContent().asString();
            }
        } catch (UnknownHostException e) {
            // DNS resolution failure — API domain does not exist
            LOG.error("UnknownHostException: API endpoint not found: "+fullURL, e.getMessage());
            // Throw the exception again
            throw new UnknownHostException("API domain not found: "+fullURL+ e.getMessage());
        } catch (ConnectException e) {
            // Network error or API not reachable (e.g. timeout, refused connection)
            LOG.error("ConnectException: Could not connect to API: "+ fullURL,e.getMessage());
        } catch (SocketTimeoutException e) {
            // API is very slow or not responding
            LOG.error("SocketTimeoutException: API did not respond in time: "+ fullURL,e.getMessage());
        } catch (SSLException e) {
            // SSL errors (certs, handshake, etc.)
            LOG.error("SSLException: SSL error while connecting to API: "+ fullURL,e.getMessage());
        } catch (IOException e) {
            // General I/O error
            LOG.error("IOException: General IO failure while accessing API: "+ fullURL,e.getMessage());
        } catch (Exception e) {
            // Unexpected error
            LOG.error("Unexpected exception: "+ e.getMessage());
        }

        return content;
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

    public String getProxy() {
        return proxy;
    }

    public String getToken() {
        return token;
    }

    public int getTimeoutSec() {
        return timeoutSec;
    }

    public boolean isVerify() {
        return verify;
    }

    public void setProxy(String proxy) {
        this.proxy = proxy;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public void setTimeoutSec(int timeoutSec) {
        this.timeoutSec = timeoutSec;
    }

    public void setVerify(boolean verify) {
        this.verify = verify;
    }


}
