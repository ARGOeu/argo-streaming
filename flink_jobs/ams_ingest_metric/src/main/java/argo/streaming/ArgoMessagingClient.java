package argo.streaming;

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
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;

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
	}

	public ArgoMessagingClient(String method, String token, String endpoint, String project, String sub, int batch,
			boolean verify) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {

		this.proto = method;
		this.token = token;
		this.endpoint = endpoint;
		this.project = project;
		this.sub = sub;
		this.maxMessages = String.valueOf(batch);
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
	 * Create an SSL Connection Socket Factory with a strategy to trust self signed
	 * certificates
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
		while ((rLine = bRdr.readLine()) != null)
			result.append(rLine);
		isRdr.close();
		Log.warn("ApiStatusCode={}, ApiErrorMessage={}", statusCode, result);

	}

	/**
	 * Properly compose url for each AMS request
	 */
	public String composeURL(String method) {
		return proto + "://" + endpoint + "/v1/projects/" + project + "/subscriptions/" + sub + ":" + method + "?key="
				+ token;
	}

	/**
	 * Executes a pull request against AMS api
	 */
	public MsgAck doPull() throws IOException, KeyManagementException, NoSuchAlgorithmException, KeyStoreException {

		ArrayList<String> msgList = new ArrayList<String>();
		ArrayList<String> ackIdList = new ArrayList<String>();

		// Create the http post to pull
		HttpPost postPull = new HttpPost(this.composeURL("pull"));
		StringEntity postBody = new StringEntity(
				"{\"maxMessages\":\"" + this.maxMessages + "\",\"returnImmediately\":\"true\"}");
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

			while ((rLine = bRdr.readLine()) != null)
				result.append(rLine);

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
	 * Executes an Acknowledge request against AMS api
	 */
	public String doAck(String ackId) throws IOException {

		// Create the http post to ack
		HttpPost postAck = new HttpPost(this.composeURL("acknowledge"));
		StringEntity postBody = new StringEntity("{\"ackIds\":[" + ackId + "]}");
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

			while ((rLine = bRdr.readLine()) != null)
				result.append(rLine);

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
	 * Close AMS http client
	 */
	public void close() throws IOException {
		this.httpClient.close();
	}
}
