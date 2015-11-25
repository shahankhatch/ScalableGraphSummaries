package edu.toronto.cs.sgbhadoop.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.commons.codec.digest.DigestUtils;

public class GeneralUtil {

	static boolean includeWarnings = false;
	static boolean showViolations = false;

	public static String getMD5(String inString) {
		return DigestUtils.md5Hex(inString);
	}


	/**
	 * This method should be used over others because it -safely- and accurately trims the host name by 1.
	 */
	public static String getHostCore(final String quadGraph) throws URISyntaxException {
		//		if (quadGraph == null) {
		//			System.out.println("null");
		//		}

		final String[] graphurisplit = quadGraph.split("\\.");
		StringBuilder host = new StringBuilder();

		try {
			Integer.valueOf(graphurisplit[graphurisplit.length - 1]);
			host.append(quadGraph);
		} catch (NumberFormatException e) {
			for (int i = Math.min(1, Math.max(graphurisplit.length - 2, 0)); i < graphurisplit.length; i++) {
				host.append(graphurisplit[i] + ".");
			}
			host.deleteCharAt(host.length() - 1);
		}
		// String[] graphurisplit = quadGraph.toString().split("\\.");
		// final String host = ((Resource) quadGraph).getHost();

		/*
		if (graphurisplit.length > 1) {
			host = graphurisplit[graphurisplit.length - 2] + "." + graphurisplit[graphurisplit.length - 1];
		} else if (graphurisplit.length == 1) {
			host = graphurisplit[0];
		} else {
			host = "";
		}
		*/
		return host.toString();
	}

	public static String getModuleContent(String location) {
		String ret = null;
		//		System.out.println("Loading module:" + location);
		try {
			final File hxmlModuleFile = new File(location);
			final InputStream in;
			if (hxmlModuleFile.exists()) {
				in = new FileInputStream(hxmlModuleFile);
			} else {
				in = GeneralUtil.class.getClassLoader().getResourceAsStream(location);
			}
			if (in == null) {
				System.out.println("Module load: input stream is null");
			} else {
				ret = readFileIntoString(in);
				in.close();
			}
		} catch (Exception e) {
			System.err.println("Module " + location + " not found. Searched the following classpath.");
			System.out.println("java.class.path:" + System.getProperty("java.class.path", null));
			e.printStackTrace();
		}
		return ret;
	}

	public static String readFileIntoString(InputStream in) {
		final StringBuffer fileContents = new StringBuffer();
		try {
			final BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line;
			while ((line = br.readLine()) != null) {
				fileContents.append(line + "\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fileContents.toString();
	}

}
