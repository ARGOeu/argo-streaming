package argo.streaming;

import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.hadoop.fs.Path;

import com.esotericsoftware.minlog.Log;

import argo.avro.MetricData;


public class TSBucketer implements Bucketer<MetricData> {

	private static final long serialVersionUID = 1L;

	/**
	 * Create a specific bucket based on the timestamp field of element
	 */
	@Override
	public Path getBucketPath(final Clock clock, final Path basePath, final MetricData element) {

		String dailyPart = element.getTimestamp().split("T")[0];
		return new Path(basePath + "/" + dailyPart);
	}
}