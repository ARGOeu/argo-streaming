package ams.connector;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom source to connect to AMS service. Uses ArgoMessaging client
 */
public class ArgoMessagingSource extends RichSourceFunction<String> {

	private static final long serialVersionUID = 1L;

	// setup logger
	static Logger LOG = LoggerFactory.getLogger(ArgoMessagingSource.class);

	private String endpoint = null;
	private String port = null;
	private String token = null;
	private String project = null;
	private String sub = null;
	private int batch = 1;
	private long interval = 100L;
	private boolean verify = true;
	private boolean useProxy = false;
	private String proxyURL = "";
	private transient Object rateLck; // lock for waiting to establish rate
	private boolean advanceOffset = true;
	

	private volatile boolean isRunning = true;

	private ArgoMessagingClient client = null;
	private String runDate;


	public ArgoMessagingSource(String endpoint, String port, String token, String project, String sub, int batch, Long interval, String runDate) {
		this.endpoint = endpoint;
		this.port = port;
		this.token = token;
		this.project = project;
		this.sub = sub;
		this.interval = interval;
		this.batch = batch;
		this.verify = true;
		this.runDate=runDate;

	}
	
	// second constructor with advanceOffset parametter
	public ArgoMessagingSource(String endpoint, String port, String token, String project, String sub, int batch, Long interval, String runDate, boolean advanceOffset) {
		this.endpoint = endpoint;
		this.port = port;
		this.token = token;
		this.project = project;
		this.sub = sub;
		this.interval = interval;
		this.batch = batch;
		this.verify = true;
		this.runDate=runDate;
		this.advanceOffset = advanceOffset;

	}

	/**
	 * Set verify to true or false. If set to false AMS client will be able to contact AMS endpoints that use self-signed certificates
	 */
	public void setVerify(boolean verify) {
		this.verify=verify;
	}
	/**
	 * Set proxy details for AMS client
	 */
	public void setProxy(String proxyURL) {
		this.useProxy = true;
		this.proxyURL = proxyURL;
	}

	/**
	 * Unset proxy details for AMS client
	 */
	public void unsetProxy(String proxyURL) {
		this.useProxy = false;
		this.proxyURL = "";
	}


	@Override
	public void cancel() {
		isRunning = false;

	}

	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		// This is the main run logic
		while (isRunning) {
			String[] res = this.client.consume();
			if (res.length > 0) {
				for (String msg : res) {
					ctx.collect(msg);
				}

			}
			synchronized (rateLck) {
				rateLck.wait(this.interval);
			}

		}

	}

	/**
	 * AMS Source initialization
	 */
	@Override
	public void open(Configuration parameters) throws Exception {
		// init rate lock
		rateLck = new Object();
		// init client
		String fendpoint = this.endpoint;
		if (this.port != null && !this.port.isEmpty()) {
			fendpoint = this.endpoint + ":" + port;
		}
		try {
			client = new ArgoMessagingClient("https", this.token, fendpoint, this.project, this.sub, this.batch, this.verify, this.runDate);
			if (this.useProxy) {
				client.setProxy(this.proxyURL);
			}
            
			// if advanceOffset is set to true (default) advance the offset to latest or based to the run date provided
			if (advanceOffset) {
				// get the offset of the subscription, that corresponds to the date
				int offset=client.offset(); 
				// mofify the offset of the subscription to point to the offset index of the date. 
				// if date is null then the index points to the latest offset (max)
                               client.modifyOffset(offset); 
		 	}
			
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