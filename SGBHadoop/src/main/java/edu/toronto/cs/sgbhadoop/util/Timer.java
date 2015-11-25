package edu.toronto.cs.sgbhadoop.util;

import java.text.DecimalFormat;

public class Timer {

	private static final DecimalFormat df = new DecimalFormat("#.###");

	private final String label;
	private double start;
	private double end;
	boolean flagRunning = false;

	public Timer(String label, boolean startImmediately) {
		this(label);
		if (startImmediately)
			this.start();
	}

	public Timer(String label) {
		this.label = label;
	}

	public String start() {
		flagRunning = true;
		start = System.currentTimeMillis();
		return "Timer(currentTimeMillis) [" + label + "], STARTING: (" + String.format("%.0f", start) + ")";
	}

	public Timer stop() {
		flagRunning = false;
		end = System.currentTimeMillis();
		return this;
	}

	public double getTime() {
		if (flagRunning) {
			return System.currentTimeMillis() - start;
		}
		return end - start;
	}

	@Override
	public String toString() {
		double time = getTime();
		double times = time / 1000;
		double timem = times / 60;
		double timeh = timem / 60;
		String running = flagRunning ? "running" : "stopped";
		return "Timer(h,m,s,ms) [" + label + "]," + running + ": (" + df.format(timeh) + ", " + df.format(timem) + ", " + df.format(times) + ", " + time + ")";
	}
}