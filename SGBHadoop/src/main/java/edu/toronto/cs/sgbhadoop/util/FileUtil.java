package edu.toronto.cs.sgbhadoop.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.channels.Channels;
import java.util.zip.GZIPInputStream;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import sun.nio.cs.StreamDecoder2;

public class FileUtil {
	static int sz = 4096*1000;
	
	static void setFinalStatic(Field field, Object newValue) throws Exception {
		/*
		// how to use
				try {
			setFinalStatic(StreamDecoder.class.getField("DEFAULT_BYTE_BUFFER_SIZE"), sz);
		} catch (Exception ex) {
			System.err.println("Cannot modify StreamDecoder buffer size using reflection");
		}
		*/
      field.setAccessible(true);

      Field modifiersField = Field.class.getDeclaredField("modifiers");
      modifiersField.setAccessible(true);
      modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

      field.set(null, newValue);
   }
	
	public static BufferedReader getBufferedReader(String inputPath) throws FileNotFoundException, IOException {
		//		FileChannel fc = Channels.newChannel(in)
		//				GZIPInputStream gis = new GZIPInputStream(Channels.newInputStream(fc));
		if (inputPath.equals("sysin")) {
			return new BufferedReader(new InputStreamReader(System.in));
		}
		InputStream is = new FileInputStream(inputPath);
		is = Channels.newInputStream(((FileInputStream) is).getChannel());
		if (inputPath.endsWith(".gz")) {			
//			GzipCompressorInputStream is2 = new GzipCompressorInputStream(is);
			is = new GZIPInputStream(is,sz);
		} else if (inputPath.endsWith(".bz2")) {
			is = new BZip2CompressorInputStream(is);
		} else {
//			is = Channels.newInputStream(((FileInputStream) is).getChannel());
		}
		
		StreamDecoder2.DEFAULT_BYTE_BUFFER_SIZE = sz;

//		BufferedReader bis = new BufferedReader(new InputStreamReader(is),sz);
		CustomBufferedReader bis = new CustomBufferedReader(StreamDecoder2.forInputStreamReader(is, is, "UTF-8"),sz);
		
		return (BufferedReader) bis;
	}

	public static PrintWriter getPrintWriter(String outputPath) throws FileNotFoundException, IOException {
		OutputStream oFile = new FileOutputStream(outputPath);
		if (outputPath.endsWith(".gz")) {
			oFile = new GzipCompressorOutputStream(oFile);
		} else if (outputPath.endsWith(".bz2")) {
			oFile = new BZip2CompressorOutputStream(oFile);
		} else {
			oFile = Channels.newOutputStream(((FileOutputStream) oFile).getChannel());
		}
		PrintWriter pwgraph = new PrintWriter(new BufferedWriter(new OutputStreamWriter(oFile)));
		return pwgraph;
	}

	public static String getIterationAppend(String in, int iteration) {
		return in + "-" + iteration;
	}

	public static String getIterationAppend(String in, String iteration) {
		return in + "-" + iteration;
	}

	public static void deleteFolder(String folder) {
		deleteFolder(new File(folder));
	}

	public static void deleteFolder(File folder) {
		File[] files = folder.listFiles();
		if (files != null) { //some JVMs return null for empty dirs
			for (File f : files) {
				if (f.isDirectory()) {
					deleteFolder(f);
				} else {
					f.delete();
				}
			}
		}
		folder.delete();
	}

	public static String hash(String in) {
		return DigestUtils.md5Hex(in.getBytes());
	}

	public static String defaultHash() {
		return hash("0");
	}

	public static void prepareOutputFolder(final String outputPath) {
		File fOutput = new File(outputPath);
		deleteFolder(fOutput);
		fOutput.mkdirs();
	}

	public static void runShellCommand(String directory, String command2) {
		directory = directory.replace("\\", "/");
		String command1 = "sh.exe";

		ProcessBuilder r = new ProcessBuilder(command1, "-c", command2);
		r.directory(new File(directory));
		r.inheritIO();
		int waitFor = -1;
		try {
			waitFor = r.start().waitFor();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		command1 = command1.replace(".exe", "");
		command2 = command2.replace(".exe", "");

		// try alternate method for linux
		if (waitFor != 0) {
			ProcessBuilder r2 = new ProcessBuilder(command1, "-c", command2);
			r2.directory(new File(directory));
			r2.inheritIO();
			try {
				waitFor = r2.start().waitFor();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			if (waitFor != 0) {
				System.err.println("command could not execute, terminating. command was:");
				System.err.println(command2);
			}
		}

	}
}
