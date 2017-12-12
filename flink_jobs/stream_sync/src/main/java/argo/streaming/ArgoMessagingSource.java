package argo.streaming;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Custom source to connect to AMS service 
// Uses ArgoMessaging client
public class ArgoMessagingSource extends RichSourceFunction<String> {

	// setup logger
	static Logger LOG = LoggerFactory.getLogger(ArgoMessagingSource.class);
	
	private String endpoint = null;
	private String port = null;
	private String token = null;
	private String project = null;
	private String sub = null;
	private transient Object rateLck; //lock for waiting to establish rate 

	private volatile boolean isRunning = true;

	private ArgoMessagingClient client = null;

	@Override
	public void cancel() {
		isRunning = false;

	}
	
	
	@Override
	public void run(SourceContext<String> ctx)
			throws Exception {
		// This is the main run logic
		while (isRunning){
			String res = this.client.consume();
			if (res != ""){
					ctx.collect(res);
			}
			synchronized (rateLck) {
				rateLck.wait(100L);
			}
			
		}

	}

	public ArgoMessagingSource(String endpoint, String port, String token, String project, String sub) {
		this.endpoint = endpoint;
		this.port = port;
		this.token = token;
		this.project = project;
		this.sub = sub;

	}

	
	// Initializes 
	@Override
	public void open(Configuration parameters) throws Exception {
		// init rate lock
		rateLck = new Object();
		// init client
		String fendpoint=this.endpoint;
		if (this.port!=null && !this.port.isEmpty()){
			fendpoint = this.endpoint + ":" + port; 
		}
		try {
			client = new ArgoMessagingClient("https", this.token, fendpoint, this.project, this.sub);
		} catch (KeyManagementException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (KeyStoreException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() throws Exception {
		if (this.client != null) {
			client.close();
		}
		synchronized (rateLck) {
			rateLck.notify();
		}
	}

}
