package argo.mon.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class KeycloakClient {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public String retrieveAccessToken(String keycloakUrl, String clientId, String secret) {

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
                            "&client_id=" + URLEncoder.encode(clientId, String.valueOf(StandardCharsets.UTF_8)) +
                            "&client_secret=" + URLEncoder.encode(secret, String.valueOf(StandardCharsets.UTF_8)) +
                            "&scope=" + URLEncoder.encode("openid entitlements", String.valueOf(StandardCharsets.UTF_8));

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
                System.err.println(
                        "Failed to retrieve Keycloak access token. " +
                                "Response code: " + responseCode +
                                ", response: " + response
                );
                return null;
            }

            JsonNode jsonNode = objectMapper.readTree(response.toString());

            JsonNode accessToken = jsonNode.get("access_token");

            if (accessToken == null || accessToken.isNull() || accessToken.asText() == null || accessToken.asText().trim().isEmpty()) {

                System.err.println("Keycloak response does not contain a valid access_token. " +"Response: " + response);

                return null;
            }

            return accessToken.asText();

        } catch (Exception e) {
            System.err.println(
                    "Exception while retrieving Keycloak access token: "
                            + e.getMessage()
            );
            e.printStackTrace();

            return null;

        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }
}