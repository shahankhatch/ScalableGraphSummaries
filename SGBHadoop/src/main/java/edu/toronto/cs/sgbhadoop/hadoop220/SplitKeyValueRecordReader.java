package edu.toronto.cs.sgbhadoop.hadoop220;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;

public class SplitKeyValueRecordReader extends LineRecordReader {

	// skip the first key
	// it may be a continuation from a prefix split (and may include multiple lines in this file)
	// this has been placed in the custom record reader - SplitKeyValueRecordReader
	// (1) start reading from start position but
	// (2) if it is not the start of the file, skip first line and first key
	// (3) process until first key that appears past end position (it may be the same key as prior to end) 
	// then stop

	Text currentValue;

	public SplitKeyValueRecordReader() {
		super();
	}

	public SplitKeyValueRecordReader(byte[] b) {
		super(b);
	}

	private long start;
	private long pos;
	private long end;
	private LineReader in;
	private FSDataInputStream fileIn;
	private Seekable filePosition;
	private int maxLineLength;
	private LongWritable key;
	private Text value;
	private boolean isCompressedInput;
	private Decompressor decompressor;
	private byte[] recordDelimiterBytes;
	String prevKey1 = "";

	boolean flagEnd = false;
	boolean flagEndOnceOnly = true;

	@Override
	public boolean nextKeyValue() throws IOException {
		if (flagFirst) {
			flagFirst = false;
			return true;
		}
		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);
		if (value == null) {
			value = new Text();
		}
		int newSize = 0;
		// We always read one extra line, which lies outside the upper
		// split limit i.e. (end - 1)
		// sk - now continues to read lines past the upper split limit as long as it is for the first key 
		// that last appears after the limit 
		String currentKey1 = "";
		while (true) {

			if (getFilePosition() > end)
				flagEnd = true;

			newSize = in.readLine(value, maxLineLength, Math.max(maxBytesToConsume(pos), maxLineLength));
			if (newSize == 0) {
				key = null;
				value = null;
				return false;
			}
			if (value.charAt(0) != '<') {
				System.out.println("skipping invalid line without key:" + value);
				continue;
			}
			String parse[] = value.toString().split("\t| ");
			currentKey1 = parse[0].substring(0, parse[0].length() - 1);

			if (flagEnd) {
				if (flagEndOnceOnly) {
					prevKey1 = currentKey1;
					flagEndOnceOnly = false;
				}
				if (!flagEndOnceOnly && !prevKey1.equals(currentKey1)) {
					key = null;
					value = null;
					return false;
				}
				//				System.err.println("Processing lastKey:" + currentKey1);
			}

			if (!flagEnd) {
				prevKey1 = currentKey1;
			}

			pos += newSize;
			if (newSize < maxLineLength) {
				break;
			}

			// line too long. try again
			//			LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
		}
		// else {
		return true;
		//		}
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();

		// open the file and seek to the start of the split
		final FileSystem fs = file.getFileSystem(job);
		fileIn = fs.open(file);

		CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
		if (null != codec) {
			isCompressedInput = true;
			decompressor = CodecPool.getDecompressor(codec);
			if (codec instanceof SplittableCompressionCodec) {
				final SplitCompressionInputStream cIn = ((SplittableCompressionCodec) codec).createInputStream(fileIn, decompressor, start, end, SplittableCompressionCodec.READ_MODE.BYBLOCK);
				if (null == this.recordDelimiterBytes) {
					in = new LineReader(cIn, job);
				} else {
					in = new LineReader(cIn, job, this.recordDelimiterBytes);
				}

				start = cIn.getAdjustedStart();
				end = cIn.getAdjustedEnd();
				filePosition = cIn;
			} else {
				if (null == this.recordDelimiterBytes) {
					in = new LineReader(codec.createInputStream(fileIn, decompressor), job);
				} else {
					in = new LineReader(codec.createInputStream(fileIn, decompressor), job, this.recordDelimiterBytes);
				}
				filePosition = fileIn;
			}
		} else {
			fileIn.seek(start);
			if (null == this.recordDelimiterBytes) {
				in = new LineReader(fileIn, job);
			} else {
				in = new LineReader(fileIn, job, this.recordDelimiterBytes);
			}

			filePosition = fileIn;
		}
		// If this is not the first split, we always throw away first record
		// because we always (except the last split) read one extra line in
		// next() method.
		if (start != 0) {
			start += in.readLine(new Text(), 0, maxBytesToConsume(start));
		}
		this.pos = start;

		// read past the first key
		initialize2();
	}

	private int maxBytesToConsume(long pos) {
		return isCompressedInput ? Integer.MAX_VALUE : (int) Math.min(Integer.MAX_VALUE, end - pos);
	}

	private long getFilePosition() throws IOException {
		long retVal;
		if (isCompressedInput && null != filePosition) {
			retVal = filePosition.getPos();
		} else {
			retVal = pos;
		}
		return retVal;
	}

	public void initialize2() throws IOException {

		// skip the first key
		// it may be a continuation from a prefix split (and may include multiple lines in this file)
		// (1) start reading from start position but
		// (2) if it is not the start of the file, skip first line and first key
		// (3) process until end position
		// (4) continue past end position if it is the same key
		// (4)(a) if same key persists for one line only, process line, then next key, then stop
		// (4)(b) otherwise process the lines of the key, then stop

		if (start == 0) {
			return;
		}

		if (!nextKeyValue())
			return; // get first line

		String firstLine = getCurrentValue().toString();
		String parse[] = firstLine.split("\t| ");
		String firstKey = parse[0].substring(0, parse[0].length() - 1);
		//		System.err.println("Skipping firstKey:" + firstKey);
		String currKey = firstKey;
		while (firstKey.equals(currKey)) {
			if (!nextKeyValue())
				return; // get next line
			String currLine = this.getCurrentValue().toString();
			String currParse[] = currLine.split("\t| ");
			currKey = currParse[0].substring(0, currParse[0].length() - 1);
		}
		// next call to nextKeyValue() should not execute 
		// because the last time it ran it produced a line with a different key
		// and needs to be returned
		flagFirst = true;
	}

	boolean flagFirst = false;

	@Override
	public LongWritable getCurrentKey() {
		return null;
	}

	@Override
	public Text getCurrentValue() {
		return value;
	}

	@Override
	public void close() throws IOException {
		super.close();
		if (fileIn != null)
			fileIn.close();
	}

}
