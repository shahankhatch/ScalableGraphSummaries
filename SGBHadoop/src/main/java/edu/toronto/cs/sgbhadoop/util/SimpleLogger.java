package edu.toronto.cs.sgbhadoop.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class SimpleLogger {

	private static SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SS");

	public static String calcDate(long millisecs) {
		Date resultdate = new Date(millisecs);
		return fmt.format(resultdate);
	}

	private String logFilename;

	/**
	 * @return the logFilename
	 */
	public String getLogFilename() {
		return logFilename;
	}

	/**
	 * @param logFilename the logFilename to set
	 */
	public void setLogFilename(String logFilename) {
		this.logFilename = logFilename;
	}

	public static interface FormatterI {

		public String format(int level, String message);

		public String format(String message);
	}

	public static class MySimpleFormatter implements FormatterI {

		@Override
		public String format(int level, String message) {
			return format("Level " + level + "," + message);
		}

		@Override
		public String format(String record) {
			return calcDate(System.currentTimeMillis()) + "," + record;
		}

	}

	public static interface HandlerI {

		public int filterLevel = 0;

		public FormatterI getFormatter();

		public void setFormatter(FormatterI formatter);

		public void log(int level, String message);
	}

	public static abstract class Handler implements HandlerI {

		public FormatterI formatter;

		@Override
		public FormatterI getFormatter() {
			return formatter;
		}

		@Override
		public void setFormatter(FormatterI newFormatter) {
			formatter = newFormatter;
		}

		@Override
		public void log(int level, String message) {
			if (level >= filterLevel) {
				log(formatter.format(level, message));
			}
		}

		public abstract void log(String message);
	}

	public static class MyFileHandler extends Handler {

		PrintWriter printWriter;

		public MyFileHandler(String filename) throws IOException {
			printWriter = FileUtil.getPrintWriter(filename);
		}

//		public MyFileHandler(String filename, boolean append) throws IOException {
//			printWriter = FileUtil.getPrintWriter(filename, append);
//		}

		@Override
		public synchronized void log(String record) {
			printWriter.write(formatter.format(record) + "\n");
			printWriter.flush();
		}

		public void close() {
			printWriter.close();
		}
	}

	public static class MyConsoleHandler extends Handler {

		@Override
		public synchronized void log(String record) {
			System.out.println(record);
		}
	}

	ArrayList<HandlerI> handlers = new ArrayList<HandlerI>();

	public void setup(String filestring) throws IOException {
		setup(filestring, true, false);
	}

	public void setup(String filestring, boolean appendTimestampToFilename, boolean appendtoFile) throws IOException {
		// Get the global logger to configure it
		String env = System.getenv("logger.level");
		if (env != null && env.trim().length() > 0) {
			try {
			} catch (IllegalArgumentException e) {
				System.err.println("logger.level environment variable set with an invalid logger level.");
				System.exit(1);
			}
		}

		        handlers.add(new MyConsoleHandler()); 
		if (appendTimestampToFilename) {
			setLogFilename(filestring + "-" + calcDate(System.currentTimeMillis()) + ".txt");
		} else {
			setLogFilename(filestring);
		}
		System.out.println("logging to " + getLogFilename());
		handlers.add(new MyFileHandler(getLogFilename()));

		MySimpleFormatter f = new MySimpleFormatter();
		for (HandlerI h : handlers) {
			h.setFormatter(f);
		}

	}

	public void log(String message) {
		for (HandlerI h : handlers) {
			h.log(h.filterLevel, message);
		}
	}
}
