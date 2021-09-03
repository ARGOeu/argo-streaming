/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.amr;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;

/**
 *
 * Establish a connection to the given url and request data
 */
public class RequestManager {

    private String proxy;
    private String token;
    private int timeoutSec;
    private boolean verify;

    public RequestManager(String proxy, String token, int timeoutSec, boolean verify) {
        this.proxy = proxy;
        this.token = token;
        this.timeoutSec = timeoutSec;
        this.verify = verify;
    }

    public RequestManager(String proxy, String token) {
        this.proxy = proxy;
        this.token = token;
        this.timeoutSec = 5;
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
    public String getResource(String fullURL) {

        Request r = Request.Get(fullURL).addHeader("Accept", "application/json").addHeader("Content-type",
                "application/json").addHeader("x-api-key", this.token);
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
        } catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
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
