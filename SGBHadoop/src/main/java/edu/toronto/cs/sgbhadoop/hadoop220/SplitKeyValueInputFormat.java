package edu.toronto.cs.sgbhadoop.hadoop220;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.google.common.base.Charsets;

public class SplitKeyValueInputFormat extends TextInputFormat {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		FileSplit fileSplit = (FileSplit) split;
		String fileName = fileSplit.getPath().toString();
		String taskName = context.getTaskAttemptID().toString();

		System.out.println("Handling [Taskname,Filename]:[" + taskName + "," + fileName + "]");

		String delimiter = context.getConfiguration().get("textinputformat.record.delimiter");
		byte[] recordDelimiterBytes = null;
		if (null != delimiter)
			recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);

		return new SplitKeyValueRecordReader(recordDelimiterBytes);
	}

}
