package argo.mon.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;


public class ArgoMonApiInitializer implements Serializable {
    static Logger LOG = LoggerFactory.getLogger(ArgoMonApiInitializer.class);

    private Boolean checkFeed=false;
    private String keycloakUrl;
    private String argoMonApiEndpoint;
    private String argoMonClientSecret;
    private String argoMonClientID;
    private int argoMonApiTimeout;
    private String argoMonApiProxy="";
    private String tenant;
    private  String argoMonApiToken ;
    public Boolean getCheckFeed() {
        return checkFeed;
    }

    public void setCheckFeed(Boolean checkFeed) {
        this.checkFeed = checkFeed;
    }

    public String getKeycloakUrl() {
        return keycloakUrl;
    }

    public void setKeycloakUrl(String keycloakUrl) {
        this.keycloakUrl = keycloakUrl;
    }

    public String getArgoMonApiEndpoint() {
        return argoMonApiEndpoint;
    }

    public void setArgoMonApiEndpoint(String argoMonApiEndpoint) {
        this.argoMonApiEndpoint = argoMonApiEndpoint;
    }

    public String getArgoMonClientSecret() {
        return argoMonClientSecret;
    }

    public void setArgoMonClientSecret(String argoMonClientSecret) {
        this.argoMonClientSecret = argoMonClientSecret;
    }

    public String getArgoMonClientID() {
        return argoMonClientID;
    }

    public void setArgoMonClientID(String argoMonClientID) {
        this.argoMonClientID = argoMonClientID;
    }

    public int getArgoMonApiTimeout() {
        return argoMonApiTimeout;
    }

    public void setArgoMonApiTimeout(int argoMonApiTimeout) {
        this.argoMonApiTimeout = argoMonApiTimeout;
    }

    public String getArgoMonApiProxy() {
        return argoMonApiProxy;
    }

    public void setArgoMonApiProxy(String argoMonApiProxy) {
        this.argoMonApiProxy = argoMonApiProxy;
    }

//    public String getRunDate() {
//        return runDate;
//    }
//
//    public void setRunDate(String runDate) {
//        this.runDate = runDate;
//    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }


    public String getArgoMonApiToken() {
        return argoMonApiToken;
    }

    public void setArgoMonApiToken(String argoMonApiToken) {
        this.argoMonApiToken = argoMonApiToken;
    }
    public void setArgoMonApiToken() {
        this.argoMonApiToken =keycloakToken();
    }


    public boolean hasStatusApiParams() {

        return keycloakUrl != null
                && argoMonApiEndpoint != null
                && argoMonClientSecret != null
                && argoMonClientID != null;
    }

    private String keycloakToken() {
        KeycloakClient keycloakClient = new KeycloakClient();
        try {
            return keycloakClient.retrieveAccessToken(keycloakUrl, argoMonClientID, argoMonClientSecret);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }



}
