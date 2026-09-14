package argo.mon.api;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class KeycloakClient {
    static Logger LOG = LoggerFactory.getLogger(ArgoMonApiInitializer.class);
    public String retrieveAccessToken(String keycloakUrl, String clientId, String secret)
            throws IOException {

        HttpURLConnection conn = null;

        try {
            URL url = new URL(keycloakUrl);
            conn = (HttpURLConnection) url.openConnection();

            conn.setRequestMethod("POST");
            conn.setRequestProperty(
                    "Content-Type",
                    "application/x-www-form-urlencoded"
            );
            conn.setDoOutput(true);

            String body =
                    "grant_type=client_credentials" +
                            "&client_id=" + URLEncoder.encode(
                            clientId, String.valueOf(StandardCharsets.UTF_8)) +
                            "&client_secret=" + URLEncoder.encode(
                            secret, String.valueOf(StandardCharsets.UTF_8)) +
                            "&scope=" + URLEncoder.encode(
                            "openid entitlements",
                            String.valueOf(StandardCharsets.UTF_8));

            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes(StandardCharsets.UTF_8));
            }

            int responseCode = conn.getResponseCode();

            InputStream inputStream = responseCode >= 200 && responseCode < 300
                    ? conn.getInputStream()
                    : conn.getErrorStream();

            StringBuilder response = new StringBuilder();

            if (inputStream != null) {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {

                    String line;
                    while ((line = reader.readLine()) != null) {
                        response.append(line);
                    }
                }
            }

            if (responseCode < 200 || responseCode >= 300) {
                LOG.error(
                        "Failed to retrieve Keycloak access token. "
                                + "Response code: " + responseCode
                                + ", response: " + response
                );

                throw new IOException(
                        "Failed to retrieve Keycloak access token. HTTP " + responseCode
                );
            }

            JsonObject jsonObject = JsonParser
                    .parseString(response.toString())
                    .getAsJsonObject();

            if (!jsonObject.has("access_token")
                    || jsonObject.get("access_token").isJsonNull()
                    || jsonObject.get("access_token").getAsString().trim().isEmpty()) {

                LOG.error(
                        "Keycloak response does not contain a valid access_token. "
                                + "Response: " + response
                );

                throw new IOException(
                        "Keycloak response does not contain a valid access_token"
                );
            }

            return jsonObject.get("access_token").getAsString();

        } catch (IOException e) {
            LOG.error("Failed to retrieve Keycloak access token: " + e.getMessage());
            throw e;

        } catch (Exception e) {
            LOG.error(
                    "Unexpected exception while retrieving Keycloak access token: "
                            + e.getMessage(),
                    e
            );

            throw new IOException(
                    "Unable to retrieve Keycloak access token", e
            );

        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }
}