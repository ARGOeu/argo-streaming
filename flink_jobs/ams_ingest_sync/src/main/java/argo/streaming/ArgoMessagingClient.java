package argo.streaming;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
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
import com.google.gson.JsonParser;

import org.apache.http.client.methods.CloseableHttpResponse;

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
	private int batch = 100;
	private String maxMessages = "";

	// Initialize with default values
	public ArgoMessagingClient() {
		this.httpClient = HttpClients.createDefault();
		this.proto = "https";
		this.token = "token";
		this.endpoint = "localhost";
		this.project = "test_project";
		this.sub = "test_sub";
		this.batch = 100;
		this.maxMessages = "100";
	}

	// Initialized using parameters
	public ArgoMessagingClient(String method, String token, String endpoint, String project, String sub, int batch) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {

		// Create ssl context
		SSLContextBuilder builder = new SSLContextBuilder();
		builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
		SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(builder.build(),
				SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);

		this.httpClient = HttpClients.custom().setSSLSocketFactory(sslsf).build();

		this.proto = method;
		this.token = token;
		this.endpoint = endpoint;
		this.project = project;
		this.sub = sub;
		this.batch = batch;
		this.maxMessages = String.valueOf(batch);
	}

	private class MsgAck {
		String[] msgs;
		String[] ackIds;

		private MsgAck(String[] msgs, String[] ackIds) {
			this.msgs = msgs;
			this.ackIds = ackIds;
		}

		private String[] getMsgs() {
			return this.msgs;
		}

		private String[] getAckIds() {
			return this.ackIds;
		}

	}

	// Failover initializer to be called inside method (if client is not yet
	// initialized)
	private void buildHttpClient() throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
		// Create ssl context
		SSLContextBuilder builder = new SSLContextBuilder();
		builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
		SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(builder.build(),
				SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);

		this.httpClient = HttpClients.custom().setSSLSocketFactory(sslsf).build();
	}

	// Based on parameters compose the request url
	public String composeURL(String method) {
		return proto + "://" + endpoint + "/v1/projects/" + project + "/subscriptions/" + sub + ":" + method + "?key="
				+ token;
	}

	// Do a pull request
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
			buildHttpClient();
		}

		CloseableHttpResponse response = this.httpClient.execute(postPull);
		String msg = "";
		String ackId = "";
		StringBuilder result = new StringBuilder();

		HttpEntity entity = response.getEntity();

		if (entity != null) {

			InputStreamReader isRdr = new InputStreamReader(entity.getContent());
			BufferedReader bRdr = new BufferedReader(isRdr);

			String rLine;

			while ((rLine = bRdr.readLine()) != null)
				result.append(rLine);

			// Gather message from json
			JsonParser jsonParser = new JsonParser();
			// parse the json root object
			
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

		}

		response.close();

		String[] msgArr = msgList.toArray(new String[0]);
		String[] ackIdArr = ackIdList.toArray(new String[0]);

		
		
		// Return a Message array
		return new MsgAck(msgArr, ackIdArr);

	}

	// Consume = Do a pull and an acknowledge request
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

	// Do an acknowledge request based on a given ackId
	public String doAck(String ackId) throws IOException {

		// Create the http post to ack
		HttpPost postAck = new HttpPost(this.composeURL("acknowledge"));
		StringEntity postBody = new StringEntity("{\"ackIds\":[" + ackId + "]}");
		postBody.setContentType("application/json");
		postAck.setEntity(postBody);

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

		}

		response.close();

		// Return a resposeMessage
		return resMsg;

	}

	public void close() throws IOException {
		this.httpClient.close();
	}
}
