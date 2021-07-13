package argo.commons.requests;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author cthermolia
 */
public class RequestManagerTest {

    public RequestManagerTest() {
    }

    @BeforeClass
    public static void setUpClass() {
      //  assertNotNull("Test file missing", RequestManagerTest.class.getResource("/profiles/config.properties"));
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of request method, of class RequestManager.
     */
    @Test
    public void testRequest() throws Exception {
        System.out.println("request");

        String apiUri = "https://api.devel.argo.grnet.gr/api/v2";
        String key = readPropertiesFile();
        String proxy = null;
        String date = "2021-01-15";
        String metricId = "0404ce45-f20a-4cbb-b0d1-132bd6dc54ca";

        JSONObject expResult = createJson();

        String url = "/metric_profiles";

        String uri = apiUri + url + "/" + metricId;
        if (date != null) {
            uri = uri + "?date=" + date;
        }
        if (key != null && !key.equals("")) {
            JSONObject result = RequestManager.request(uri, key, proxy);
            assertEquals(expResult, result);
        }
        // TODO review the generated test code and remove the default call to fail.
        //  fail("The test case is a prototype.");
    }

    private JSONObject createJson() throws ParseException {

        String jsonString = "{\n"
                + "    \"status\": {\n"
                + "        \"message\": \"Success\",\n"
                + "        \"code\": \"200\"\n"
                + "    },\n"
                + "    \"data\": [\n"
                + "        {\n"
                + "            \"id\": \"0404ce45-f20a-4cbb-b0d1-132bd6dc54ca\",\n"
                + "            \"date\": \"2020-07-27\",\n"
                + "            \"name\": \"OPS_MONITOR_RHEL7\",\n"
                + "            \"description\": \"Profile for monitoring operational tools for RHEL 7\",\n"
                + "            \"services\": [\n"
                + "                {\n"
                + "                    \"service\": \"argo.mon\",\n"
                + "                    \"metrics\": [\n"
                + "                        \"eu.egi.CertValidity\",\n"
                + "                        \"org.nagios.NagiosWebInterface\"\n"
                + "                    ]\n"
                + "                },\n"
                + "                {\n"
                + "                    \"service\": \"argo.webui\",\n"
                + "                    \"metrics\": [\n"
                + "                        \"org.nagios.ARGOWeb-AR\",\n"
                + "                        \"org.nagios.ARGOWeb-Status\"\n"
                + "                    ]\n"
                + "                },\n"
                + "                {\n"
                + "                    \"service\": \"ch.cern.cvmfs.stratum.0\",\n"
                + "                    \"metrics\": [\n"
                + "                        \"org.nagiosexchange.CVMFS-WebCheck\"\n"
                + "                    ]\n"
                + "                },\n"
                + "                {\n"
                + "                    \"service\": \"egi.aai.oidc\",\n"
                + "                    \"metrics\": [\n"
                + "                        \"eu.egi.CertValidity\"\n"
                + "                    ]\n"
                + "                },\n"
                + "                {\n"
                + "                    \"service\": \"egi.AccountingPortal\",\n"
                + "                    \"metrics\": [\n"
                + "                        \"eu.egi.CertValidity\",\n"
                + "                        \"org.nagiosexchange.AccountingPortal-WebCheck\"\n"
                + "                    ]\n"
                + "                },\n"
                + "                {\n"
                + "                    \"service\": \"egi.AppDB\",\n"
                + "                    \"metrics\": [\n"
                + "                        \"eu.egi.CertValidity\",\n"
                + "                        \"org.nagiosexchange.AppDB-WebCheck\"\n"
                + "                    ]\n"
                + "                },\n"
                + "                {\n"
                + "                    \"service\": \"egi.GGUS\",\n"
                + "                    \"metrics\": [\n"
                + "                        \"eu.egi.CertValidity\",\n"
                + "                        \"org.nagiosexchange.GGUS-WebCheck\"\n"
                + "                    ]\n"
                + "                },\n"
                + "                {\n"
                + "                    \"service\": \"egi.GOCDB\",\n"
                + "                    \"metrics\": [\n"
                + "                        \"eu.egi.CertValidity\",\n"
                + "                        \"org.nagios.GOCDB-PortCheck\",\n"
                + "                        \"org.nagiosexchange.GOCDB-PI\",\n"
                + "                        \"org.nagiosexchange.GOCDB-WebCheck\"\n"
                + "                    ]\n"
                + "                },\n"
                + "                {\n"
                + "                    \"service\": \"egi.MetricsPortal\",\n"
                + "                    \"metrics\": [\n"
                + "                        \"org.nagiosexchange.MetricsPortal-WebCheck\"\n"
                + "                    ]\n"
                + "                },\n"
                + "                {\n"
                + "                    \"service\": \"egi.MSGBroker\",\n"
                + "                    \"metrics\": [\n"
                + "                        \"eu.egi.CertValidity\",\n"
                + "                        \"org.nagiosexchange.Broker-BDII\"\n"
                + "                    ]\n"
                + "                },\n"
                + "                {\n"
                + "                    \"service\": \"egi.OpsPortal\",\n"
                + "                    \"metrics\": [\n"
                + "                        \"eu.egi.CertValidity\",\n"
                + "                        \"org.nagiosexchange.OpsPortal-WebCheck\"\n"
                + "                    ]\n"
                + "                },\n"
                + "                {\n"
                + "                    \"service\": \"egi.Perun\",\n"
                + "                    \"metrics\": [\n"
                + "                        \"eu.egi.CertValidity\",\n"
                + "                        \"eu.egi.cloud.Perun-Check\"\n"
                + "                    ]\n"
                + "                },\n"
                + "                {\n"
                + "                    \"service\": \"egi.Portal\",\n"
                + "                    \"metrics\": [\n"
                + "                        \"eu.egi.CertValidity\",\n"
                + "                        \"org.nagiosexchange.Portal-WebCheck\"\n"
                + "                    ]\n"
                + "                },\n"
                + "                {\n"
                + "                    \"service\": \"egi.TMP\",\n"
                + "                    \"metrics\": [\n"
                + "                        \"org.nagiosexchange.TMP-WebCheck\"\n"
                + "                    ]\n"
                + "                },\n"
                + "                {\n"
                + "                    \"service\": \"eu.egi.cloud.broker.vmdirac\",\n"
                + "                    \"metrics\": [\n"
                + "                        \"eu.egi.CertValidity\"\n"
                + "                    ]\n"
                + "                },\n"
                + "                {\n"
                + "                    \"service\": \"eu.egi.notebooks.jupyterhub\",\n"
                + "                    \"metrics\": [\n"
                + "                        \"eu.egi.Notebooks-Status\"\n"
                + "                    ]\n"
                + "                },\n"
                + "                {\n"
                + "                    \"service\": \"eu.egi.rt\",\n"
                + "                    \"metrics\": [\n"
                + "                        \"eu.egi.CertValidity\",\n"
                + "                        \"org.nagiosexchange.RT-WebCheck\"\n"
                + "                    ]\n"
                + "                },\n"
                + "                {\n"
                + "                    \"service\": \"ngi.OpsPortal\",\n"
                + "                    \"metrics\": [\n"
                + "                        \"eu.egi.CertValidity\",\n"
                + "                        \"org.nagiosexchange.OpsPortal-WebCheck\"\n"
                + "                    ]\n"
                + "                },\n"
                + "                {\n"
                + "                    \"service\": \"org.onedata.oneprovider\",\n"
                + "                    \"metrics\": [\n"
                + "                        \"eu.egi.CertValidity\"\n"
                + "                    ]\n"
                + "                },\n"
                + "                {\n"
                + "                    \"service\": \"org.onedata.onezone\",\n"
                + "                    \"metrics\": [\n"
                + "                        \"eu.egi.CertValidity\"\n"
                + "                    ]\n"
                + "                }\n"
                + "            ]\n"
                + "        }\n"
                + "    ]\n"
                + "}";

        JSONParser parser = new JSONParser();
        JSONObject json = (JSONObject) parser.parse(jsonString);

        return json;

    }

    private String readPropertiesFile() throws FileNotFoundException, IOException, org.json.simple.parser.ParseException {
        String propFileName = "config1.properties";
        Properties prop = new Properties();
        InputStream inputStream = RequestManagerTest.class.getResourceAsStream("/profiles/config.properties");

        if (inputStream != null) {
            prop.load(inputStream);
        } else {
            throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
        }

        // get the property value and print it out
        String argoToken = prop.getProperty("argo.webapi.token");
        return argoToken;
    }

}
