package edu.toronto.cs.sgbhadoop.util;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class HadoopUtil {

	public static String getHDFSModuleContent(String location) {
		FileSystem fileSystem;
		try {
			fileSystem = FileSystem.get(new URI(location), new Configuration());
			return GeneralUtil.readFileIntoString(fileSystem.open(new Path(location)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "\"this is the default return\"; 1+1;";
	}

	public static FSDataInputStream getFileFromFileSplit(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
		// handle input
		FileSplit split = (FileSplit) genericSplit;

		// get the path of the split
		final Path file = split.getPath();

		//		FileSystem.get(uri, conf)
		// get the filesystem, outputting the current split's filename to stderr
		FileSystem fs = file.getFileSystem(context.getConfiguration());

		// obtain a stream of the InputSplit (a file)
		FSDataInputStream fileIn = null;
		fileIn = fs.open(split.getPath());
		System.out.println("Handling file:" + file);
		if (fileIn == null)
			throw new IOException();
		return fileIn;
	}

}
