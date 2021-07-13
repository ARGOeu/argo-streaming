package sync;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import argo.avro.Downtime;

/**
 * Downtime Cache holds multiple DowntimeManagers and it's used to keep track of
 * downtime feeds during streaming status computation (Downtime data sets for
 * later days might come earlier so must be cached.
 */
public class DowntimeCache {

	// cache holding
	private TreeMap<String, DowntimeManager> cache;
	private int maxItems;

	public DowntimeCache() {
		this.maxItems = 1;
		this.cache = new TreeMap<String, DowntimeManager>();

	}

	public DowntimeCache(int maxItems) {
		this.maxItems = maxItems;
		this.cache = new TreeMap<String, DowntimeManager>();
	}
	
	/**
	 * Returns the max number of items that current cache holds
	 */
	public int getMaxItems() {
		return this.maxItems;
	}

	/**
	 * Get for a specific day the corresponding DowntimeManager
	 */
	public DowntimeManager getDowntimeManager(String dayStamp) {
		return cache.get(dayStamp);
	}

	/**
	 * Load downtime dataset from avro file and store it in downtime cache for a 
	 * specific day
	 */
	public void addFileFeed(String dayStamp, File avroFile) throws IOException {
		// check first if the item already exists in the cache so just update it
		if (cache.containsKey(dayStamp)) {
			cache.get(dayStamp).loadAvro(avroFile);
			return;
		}

		// check if item has daystamp older than the oldest item in cache
		if (cache.size() > 0 && dayStamp.compareTo(cache.firstKey()) < 1) {
			return;
		}

		DowntimeManager downMgr = new DowntimeManager();
		downMgr.loadAvro(avroFile);
		cache.put(dayStamp, downMgr);

		// check if item insertion grew cache outside its limits
		if (cache.size() > maxItems) {
			// remove oldest
			cache.remove(cache.firstKey());
		}
	}

	/**
	 * Load downtime dataset from downtime object list and store it in downtime cache for a 
	 * specific day
	 */
	public void addFeed(String dayStamp, List<Downtime> downList) {

		// check first if the item already exists in the cache so just update it
		if (cache.containsKey(dayStamp)) {
			cache.get(dayStamp).loadFromList(downList);
			return;
		}

		// check if item has daystamp older than the oldest item in cache
		if (cache.size() > 0 && dayStamp.compareTo(cache.firstKey()) < 1) {
			return;
		}

		DowntimeManager downMgr = new DowntimeManager();
		downMgr.loadFromList(downList);
		cache.put(dayStamp, downMgr);

		// check if item insertion grew cache outside its limits
		if (cache.size() > maxItems) {
			// remove oldest
			cache.remove(cache.firstKey());
		}
	}

	/**
	 * Check if downtime period exists for a specific endpoint (service, hostname, timestamp)
	 */
	public ArrayList<String> getDowntimePeriod(String dayStamp, String hostname, String service) {
		// If downtime manager with data exists for specific day
		if (cache.containsKey(dayStamp)) {
			// return the downtime period from downtime manager of specific day
			return cache.get(dayStamp).getPeriod(hostname, service);
		}

		return null;
	}

	public void clear() {
		this.cache.clear();
	}

	public String toString() {
		return this.cache.toString();
	}

}
