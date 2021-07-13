package ops;

import java.math.BigDecimal;

public class DIntegrator {

	public double availability;
	public double reliability;

	public double up_f;
	public double unknown_f;
	public double down_f;

	public static double round(double input, int prec, int mode) {
		try {
			BigDecimal inputBD = BigDecimal.valueOf(input);
			BigDecimal rounded = inputBD.setScale(prec, mode);
			return rounded.doubleValue();

		} catch (NumberFormatException e) {
			return -1;
		}
	}

	public void clear() {
		this.up_f = 0;
		this.unknown_f = 0;
		this.down_f = 0;

		this.availability = 0;
		this.reliability = 0;
	}

	public void calculateAR(int[] samples, OpsManager opsMgr) {

		clear();

		double up = 0;
		double down = 0;
		double unknown = 0;

		for (int i = 0; i < samples.length; i++) {
			if (samples[i] == opsMgr.getIntStatus("OK")) {
				up++;
			} else if (samples[i] == opsMgr.getIntStatus("WARNING")) {
				up++;
			} else if (samples[i] == opsMgr.getIntStatus("MISSING")) {
				unknown++;
			} else if (samples[i] == opsMgr.getIntStatus("UNKNOWN")) {
				unknown++;
			} else if (samples[i] == opsMgr.getIntStatus("DOWNTIME")) {
				down++;
			} else if (samples[i] == opsMgr.getIntStatus("CRITICAL")) {

			}
		}

		double dt = samples.length;

		// Availability = UP period / KNOWN period = UP period / (Total period –
		// UNKNOWN period)
		this.availability = round(((up / dt) / (1.0 - (unknown / dt))) * 100, 5, BigDecimal.ROUND_HALF_UP);

		// Reliability = UP period / (KNOWN period – Scheduled Downtime)
		// = UP period / (Total period – UNKNOWN period – ScheduledDowntime)
		this.reliability = round(((up / dt) / (1.0 - (unknown / dt) - (down / dt))) * 100, 5, BigDecimal.ROUND_HALF_UP);

		this.up_f = round(up / dt, 5, BigDecimal.ROUND_HALF_UP);
		this.unknown_f = round(unknown / dt, 5, BigDecimal.ROUND_HALF_UP);
		this.down_f = round(down / dt, 5, BigDecimal.ROUND_HALF_UP);

	}

}
