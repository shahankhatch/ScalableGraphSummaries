package edu.toronto.cs.sgbhadoop.hadoop220;

import java.io.File;
import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.toronto.cs.sgbhadoop.sortedpackage.Sig;
import edu.toronto.cs.sgbhadoop.util.GeneralUtil;
import edu.toronto.cs.sgbhadoop.util.Timer;

/**
 * Map-focused Hadoop job for computing FWBW, reduce only sorts (in parallel if avail). Flips stmts back and forth and aggregates over a stream of key, emitting the 'join' afterwards.
 *Seems to work.
 */
public class BisimFWBW {
	public static final boolean flagDebug = false;
	//	private final boolean flagfw = false;
	//	private final boolean flagbw = false;
	public static boolean flagInFWModeofFWBW = true;
	public static final boolean flagValidateIRIs = false;

	private static final String fileextgz = ""; // .gz for zip, .bz2 for bzip2, empty for uncompressed
	private static final String fileext = ""; // .gz for zip, .bz2 for bzip2, empty for uncompressed

	private static final String MAPOUT_FILE = "file" + fileext;

	private static final String SORTED_FILE = "filesorted" + fileext;

	private static final String NODES_FILE = "nodes" + fileextgz;

	private static final String EXTENTS_FILE = "extents" + fileextgz;

	private static final String GRAPH_FILE = "graph" + fileextgz;

	private static final String FINAL_FILE = "final" + fileext;

	/**
	 * Mapper used for initializing a dataset for iterative computation
	 *
	 */
	public static class FWBWMapperInitialization extends Mapper<Object, Text, Text, Text> {

		String[] parse;
		private int outnum;

		final Text emptyText = new Text();

		Timer t;

		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
			t = new Timer("initializationmapper-" + context.getTaskAttemptID(), true);
		}

		@Override
		protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
			System.out.println("outnum final:" + outnum);
			System.out.println(t.stop());
		}

		final Text zeroline = new Text();
		final Text oneline = new Text();

		@Override
		protected void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {

			parse = value.toString().split("\t| ");

			if (parse.length != 3 && parse.length != 4 && parse.length != 5) {
				//				logger.finest("skipping line (not at least a triple):" + line);
				// require at least a triple
				return;
			}

			//			// check that it is a valid nt statement
			//			if (parse[parse.length - 1].charAt(0) != '.') {
			//				//				continue;
			//			}

			final String s;
			final String p;
			final String o;

			s = parse[0];
			p = parse[1];
			o = parse[2];

			if (o.charAt(0) != '<' || s.charAt(0) != '<' || p.charAt(0) != '<') {
				// ignore statements with literals, and check that it is somewhat well-formed
				return;
			}

			final String blockidentifier;

			if (parse.length == 3) {
				// assign an initial block identifier
				blockidentifier = "0";
			} else if (parse.length == 4 && parse[3].equals(".")) {
				// assign an initial block identifier
				blockidentifier = "0";
			} else {
				blockidentifier = parse[3];
			}

			final String lastpart = " " + p + " " + blockidentifier + " ";

			//			Text t1 = new Text(s + ". ");
			//			context.write(t1, new Text("n00"));
			//			Text t2 = new Text(o + ". ");
			//			context.write(t2, new Text("n00"));
			zeroline.set(s + "0" + lastpart + o + " .");
			oneline.set(o + "1" + lastpart + s + " .");
			//			}

			context.write(zeroline, emptyText);
			context.write(oneline, emptyText);
			outnum++;

			if (outnum % 1000000 == 0) {
				System.out.println("timer:" + t);
				System.out.println("num of lines:" + outnum);
			}
		}
	}

	/**
	 * Reducer used for sorting
	 *
	 */
	public static class IdentityReducerTask extends Reducer<Text, Text, Text, Text> {
		private MultipleOutputs<Text, Text> multipleOutputs;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<Text, Text>(context);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
		}

		final Text emptyText = new Text();
		final Text keyText = new Text();
		final Text valText = new Text();
		String bid = "";

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (key.toString().startsWith("null") || key.toString().equals("-")) {
				if (flagDebug)
					System.err.println("key is null in identity reducer task.");
				return;
			}
			//			String oldblockid = "";
			//			String newblockid = "";
			bid = "";
			for (Text val : values) {
				// write node blockids into the stream only once
				if (val.toString().startsWith("n1")) {
					bid = val.toString().substring(2);
					if (bid.length() > 0) {
						keyText.set(key.toString() + bid + " .");
						context.write(keyText, emptyText);
						multipleOutputs.write(keyText, emptyText, context.getConfiguration().get("vssp.output") + File.separatorChar + "nodes" + File.separatorChar + "nodes");
					} else {
						System.err.println("BID length is zero in reduce!");
					}
					//					if (val.toString().startsWith("n0")) {
					//						oldblockid = key.toString() + bid + " .";
					//						//						if (flagDebug) {
					//						//							System.err.println("oldblockid:" + bid);
					//						//						}
					//					} else if (val.toString().startsWith("n1")) {
					//						newblockid = key.toString() + bid + " .";
					//						//						if (flagDebug) {
					//						//							System.err.println("newblockid:" + bid);
					//						//						}
					continue;
					//					}
				} else {
					context.write(key, val);
				}

			}

			//
			//			if (newblockid.length() > 0) {
			//				if (flagDebug) {
			//					System.err.println("wrote newblockid:" + newblockid);
			//				}
			//				if (newblockid.contains("null")) {
			//					System.err.println("newblockid contains null");
			//				}
			//				context.write(new Text(newblockid), emptyText);
			//				multipleOutputs.write(new Text(newblockid), emptyText, context.getConfiguration().get("vssp.output") + File.separatorChar + "nodes" + File.separatorChar + "nodes");
			//			} else if (oldblockid.length() > 0) {
			//				if (flagDebug) {
			//					System.err.println("wrote oldblockid:" + oldblockid);
			//				}
			//				if (oldblockid.contains("null")) {
			//					System.err.println("oldblockid contains null");
			//				}
			//				context.write(new Text(oldblockid), emptyText);
			//				multipleOutputs.write(new Text(oldblockid), emptyText, context.getConfiguration().get("vssp.output") + File.separatorChar + "nodes" + File.separatorChar + "nodes");
			//			}
		}
	}

	/**
	 * Mapper to compute new ids
	 *
	 */
	public static class FWBWBisimMapperTask extends Mapper<Object, Text, Text, Text> {
		//		private MultipleOutputs<Text, Text> multipleOutputs;

		String currKeyParse;
		String prevKey = "-";
		String currentKey = "-";
		boolean pkdone = false;
		boolean ckdone = false;

		// will generate ID using a rolling hash signature
		String sig = "";

		String newblockid = null;
		String bid = null;
		String pbid = null;
		String nodeblockid = null;
		String prevnodeblockid = null;

		String s, p, o;

		final TreeSet<Sig> tsFW = new TreeSet<Sig>();

		final Text emptyText = new Text();
		final Text mapOutKeyText = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			if (context.getConfiguration().getInt("vssp.iteration", 0) % 2 == 1) {
				flagInFWModeofFWBW = true;
			} else {
				flagInFWModeofFWBW = false;
			}

			currentKey = "-";
			// key is not used
			// value contains the line

			if (flagDebug) {
				System.out.println("value:" + value);
			}

			//			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			//			if (fileName.contains("nodes")) {
			//				String parse[] = value.toString().split("\t| ");
			//				String bid = parse[1];
			//				String ckey = parse[0] + " ";
			//				System.out.println("ckey:" + ckey);
			//				if (ckey.contains("-")) {
			//					System.err.println("here");
			//				}
			//				if (!bid.equals("0") && bid.length() != 32) {
			//					System.err.println("something wrong with bid pickup");
			//				}
			//				context.write(new Text(ckey), new Text("n0" + bid));
			//				return;
			//			}

			// parse the line from the value
			String parse[] = value.toString().split("\t| ");

			// check that it is the correct length
			if ((parse.length != 5 && parse.length != 3) || parse[parse.length - 1].charAt(0) != '.') {
				System.err.println("Length != 5 encountered in reduce!:" + value);
				return;
			}

			currKeyParse = parse[0];

			currentKey = currKeyParse.substring(0, currKeyParse.length() - 1);

			char lchar = currKeyParse.charAt(currKeyParse.length() - 1);
			if (lchar == '.') {
				if (nodeblockid != null) {
					prevnodeblockid = nodeblockid;
				}
				nodeblockid = parse[1];
				if (flagDebug) {
					System.out.println("nodeblockid:" + currentKey + "," + nodeblockid);
				}
				return;
			}
			if (lchar != '0' && lchar != '1') {
				System.err.println("lchar is not 0 or 1!!");
				return;
			}

			s = currentKey;
			p = parse[1];
			o = parse[3];
			bid = parse[2];

			// initialization and state update
			if (prevKey.equals("-")) {
				prevKey = currentKey;
			}

			if (prevKey.equals(currentKey)) {
				ckdone = pkdone;
			}

			if (!prevKey.equals(currentKey) && !pkdone) {
				if (flagDebug)
					System.out.println("CASEB");
				String oldblockid = computeId(prevKey, tsFW, prevnodeblockid);
				//				System.err.println("sending n1:" + prevKey + "," + oldblockid);
				context.write(new Text(prevKey + ". "), new Text("n1" + oldblockid));

				//				multipleOutputs.write(new Text(prevKey), new Text(oldblockid), context.getConfiguration().get("vssp.output") + File.separatorChar + "nodes" + File.separatorChar + "nodes");
				pkdone = true;
			}

			// line 2-5
			if (lchar == '0') {
				// in collection phase
				Sig sig;
				if (flagInFWModeofFWBW) {
					sig = new Sig(p, bid);
				} else {
					sig = new Sig("-" + p, bid);
				}

				if (!tsFW.contains(sig)) {
					tsFW.add(sig);
				}
			}

			if (lchar == '1' && !ckdone) {
				// the current key can be id-ed
				if (flagDebug)
					System.out.println("CASEA");
				newblockid = computeId(currentKey, tsFW, nodeblockid);
				//				System.err.println("sending n1:" + currentKey + "," + newblockid);
				context.write(new Text(currentKey + ". "), new Text("n1" + newblockid));

				//				multipleOutputs.write(new Text(currentKey), new Text(newblockid), context.getConfiguration().get("vssp.output") + File.separatorChar + "nodes" + File.separatorChar + "nodes");
				ckdone = true;
			}

			// line 8-11
			if (lchar == '1') {

				// remem that '1' category has stmts of the form (s,-p,o)
				//				mapOutKeyText.set(s + "0" + lastPart + s + " .");
				mapOutKeyText.set(s + "0" + " " + p + " " + bid + " " + o + " .");
				context.write(mapOutKeyText, emptyText);

				mapOutKeyText.set(o + "1" + " " + p + " " + newblockid + " " + s + " .");
				context.write(mapOutKeyText, emptyText);
			}

			// update history
			prevKey = currentKey;
			pkdone = ckdone;
			ckdone = false;
			pbid = bid;

		}

		@Override
		protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
			// capture the last node
			if (!pkdone && !prevKey.equals("-")) {
				if (flagDebug)
					System.out.println("CASEC");
				newblockid = computeId(prevKey, tsFW, prevnodeblockid);
				//				System.err.println("sending n1:" + prevKey + "," + newblockid);
				context.write(new Text(prevKey + ". "), new Text("n1" + newblockid));

				//				multipleOutputs.write(new Text(prevKey), new Text(newblockid), context.getConfiguration().get("vssp.output") + File.separatorChar + "nodes" + File.separatorChar + "nodes");
			}

			//			multipleOutputs.close();
		}

		//		@SuppressWarnings("unchecked")
		//		@Override
		//		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
		//			multipleOutputs = new MultipleOutputs<Text, Text>(context);
		//		}

		private String computeId(String currentKey, final TreeSet<Sig> tsFW, String prevbid) {
			if (prevbid == null) {
				prevbid = "0";
			}
			String newblockid;
			if (flagDebug) {
				System.out.println("---- computeId --");
				System.out.println("currentKey:" + currentKey);
				System.out.println("prevbid:" + prevbid);
				System.out.println("hashing: " + prevbid + tsFW.toString());
				System.out.println("ts:" + tsFW);
			}
			//			newblockid = hash(tsFW.toString());
			newblockid = GeneralUtil.getMD5(prevbid + tsFW.toString());
			if (flagDebug) {
				System.out.println("computId:" + currentKey + "," + newblockid);
				System.out.println();
			}
			tsFW.clear();
			return newblockid;
		}

	}

	// app config
	private static boolean flagDoCompress = true;

	private static Job createInitJob(Configuration conf, String inPath, String outPath) throws IOException, InterruptedException, ClassNotFoundException {

		//		conf.set("mapreduce.local.map.tasks.maximum", "4");
		//		conf.set("mapreduce.tasktracker.map.tasks.maximum", "3");

		// mapreduce.input.fileinputformat.split.maxsize
		// mapreduce.input.fileinputformat.split.minsize

		FileSystem fs = FileSystem.get(conf);

		Job job = new Job(conf, "BisimFWBW-initialization");
		job.setJarByClass(BisimFWBW.class);

		if (flagDoCompress) {
			FileOutputFormat.setCompressOutput(job, true);
			//			FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.BZip2Codec.class);
			//						FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.GzipCodec.class);
			//			FileOutputFormat.setOutputCompressorClass(job, nl.basjes.hadoop.io.compress.SplittableGzipCodec.class);
		}

		//		conf.set("mapred.local.map.tasks.maximum", "4");
		System.out.println("setting local max running maps:" + conf.getInt("vssp.nummaps", 1));
		LocalJobRunner.setLocalMaxRunningMaps(job, conf.getInt("vssp.nummaps", 1));
		job.setNumReduceTasks(1);

		job.setMapperClass(FWBWMapperInitialization.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		//		job.setMapOutputKeyClass(MyCompoundKey.class);
		//		job.setSortComparatorClass(MyCompoundKeyComparator.class);
		//		job.setPartitionerClass(MyCompoundKeyPartitioner.class);
		//		job.setGroupingComparatorClass(MyCompoundKeyGroupingComparator.class);

		// used to sort 
		job.setReducerClass(IdentityReducerTask.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// requires an external index
		//		job.setPartitionerClass(TotalOrderPartitioner.class);

		FileInputFormat.addInputPath(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		MultipleOutputs.setCountersEnabled(job, true);

		// true stands for recursively deleting the folder you gave
		fs.delete(new Path(outPath), true);

		return job;
	}

	private static Configuration applyCustomConfSettings(Configuration conf, String outPath, int iteration) {
		conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.DefaultCodec,nl.basjes.hadoop.io.compress.SplittableGzipCodec,org.apache.hadoop.io.compress.BZip2Codec");

		conf.setInt("vssp.iteration", iteration);
		conf.set("vssp.output", outPath);

		// lmdb - uncompressed - for 8 threads
		//		conf.set("mapreduce.input.fileinputformat.split.minsize", 50000000 + "");
		//		conf.set("mapreduce.input.fileinputformat.split.maxsize", 50000000 + "");

		// lmdb - uncompressed
		//				conf.set("mapreduce.input.fileinputformat.split.minsize", "200000000");
		//				conf.set("mapreduce.input.fileinputformat.split.maxsize", "300000000");

		// lmdb - compressed
		conf.set("mapreduce.input.fileinputformat.split.minsize", "1000000");
		conf.set("mapreduce.input.fileinputformat.split.maxsize", "3000000");
		// dbpedia
		//		conf.set("mapreduce.input.fileinputformat.split.minsize", "900000000");
		//		conf.set("mapreduce.input.fileinputformat.split.maxsize", "1000000000");
		conf.set("mapreduce.task.io.sort.mb", "300");

		//		conf.set("java.io.tmpdir", "c:/htmp");
		//		conf.set("mapreduce.task.tmp.dir", "c:/htmp");
		//		conf.set("hadoop.tmp.dir", "c:/htmp");
		// following don't work
		//		conf.set("mapreduce.job.local.dir", "c:/htmp");
		//		conf.set("mapreduce.task.tmp.dir", "c:/htmp");

		if (flagDoCompress) {
			conf.set("mapred.compress.map.output", "true");
			conf.set("mapreduce.map.output.compress", "true");
		}
		return conf;
	}

	private static Job createTaskJob(Configuration conf, String inPath, String outPath) throws IOException, InterruptedException, ClassNotFoundException {

		Job job = new Job(conf, "BisimFWBW");
		job.setJarByClass(BisimFWBW.class);

		job.setMapperClass(FWBWBisimMapperTask.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		//		job.setMapOutputKeyClass(MyCompoundKey.class);
		//		job.setMapOutputValueClass(WritablePair.class);

		// Note: the combiner cannot be used since it cannot return distinct values across parallel tasks
		//job.setCombinerClass(UpdateReducer.class);

		//		job.setSortComparatorClass(MyCompoundKeyComparator.class);
		//		job.setPartitionerClass(MyCompoundKeyPartitioner.class);
		//		job.setGroupingComparatorClass(MyCompoundKeyGroupingComparator.class);

		// used to sort 
		job.setReducerClass(IdentityReducerTask.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//		conf.set("mapred.local.map.tasks.maximum", "4");
		System.out.println("setting local max running maps:" + conf.getInt("vssp.nummaps", 1));
		LocalJobRunner.setLocalMaxRunningMaps(job, conf.getInt("vssp.nummaps", 1));
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(inPath));
		//		FileInputFormat.addInputPath(job, new Path(conf.get("vssp.noderestorepath") + File.separatorChar + "nodes" + "*.gz")); // + File.separatorChar + "nodes"
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		if (flagDoCompress) {
			FileOutputFormat.setCompressOutput(job, true);
			//			FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.BZip2Codec.class);
			//			FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.GzipCodec.class);
			//			FileOutputFormat.setOutputCompressorClass(job, nl.basjes.hadoop.io.compress.SplittableGzipCodec.class);
		}

		// should always be split key/value reader otherwise keys may be left in other splits
		job.setInputFormatClass(SplitKeyValueInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		MultipleOutputs.setCountersEnabled(job, true);

		return job;
	}

	public static void main(String[] args) throws Exception {
		Logger.getRootLogger().setLevel(Level.INFO);

		Timer t = new Timer("fulljob", true);
		Configuration conf = new Configuration();

		if (System.getProperty("vssp.nummaps") == null) {
			System.setProperty("vssp.nummaps", "4");
		}
		System.out.println("vssp.nummaps:" + System.getProperty("vssp.nummaps"));
		System.out.println("conf.tmp:" + System.getProperty("hadoop.tmp.dir"));
		conf.set("hadoop.tmp.dir", System.getProperty("hadoop.tmp.dir"));
		conf.setInt("vssp.nummaps", Integer.parseInt(System.getProperty("vssp.nummaps")));

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: BisimFWBW <in> <out>");
			System.exit(2);
		}

		String inPath = otherArgs[0];
		String outPath = otherArgs[1];

		FileSystem fs = FileSystem.get(conf);

		int fnum = 0;
		Timer t1 = new Timer("init", true);
		fs.delete(new Path(outPath + "-" + fnum), true);
		applyCustomConfSettings(conf, outPath + "-" + fnum, fnum);
		Job job = createInitJob(conf, inPath, outPath + "-" + fnum + File.separatorChar + "graph");
		job.waitForCompletion(true);
		System.out.println(t1.stop());

		//		System.exit(job.waitForCompletion(true) ? 0 : 1);

		long countold = 1;
		long countnew = -1;
		// won't work because initialization does not produce distinct nodes, just sorted statements
		//		long countold = runCountJob(outPath + "-" + fnum + File.separatorChar + "nodes", outPath + "-" + fnum + File.separatorChar + "count");
		int countequalscount = 0;
		while (countequalscount != 3) {
			if (fnum > 2) {
				fs.delete(new Path(outPath + "-" + (fnum - 2)), true);
				fs.delete(new Path(conf.get("hadoop.tmp.dir")), true);
			}

			String f1 = outPath + "-" + fnum + File.separatorChar + "graph";
			String noderestorepath = outPath + "-" + fnum + File.separatorChar + "nodes"; // + File.separatorChar + "nodes"

			fnum++;
			String f2 = outPath + "-" + fnum;

			Timer t2 = new Timer("iter-" + fnum, true);
			applyCustomConfSettings(conf, outPath + "-" + fnum, fnum);
			conf.set("vssp.noderestorepath", noderestorepath);

			// true stands for recursively deleting the folder you gave
			fs.delete(new Path(outPath + "-" + fnum), true);

			Job jobInner = createTaskJob(conf, f1, f2 + File.separatorChar + "graph");
			jobInner.waitForCompletion(true);
			System.out.println(t2.stop());

			Timer t3 = new Timer("count-" + fnum, true);
			countold = countnew;
			countnew = runCountJob(f2 + File.separatorChar + "nodes", f2 + File.separatorChar + "count");
			if (countold == countnew) {
				countequalscount++;
			} else {
				countequalscount = 0;
			}
			System.out.println(t3.stop());
			System.err.println("counter:" + countnew);
		}

		System.out.println(t.stop());
	}

	private static long runCountJob(String inPath, String outPath) throws IllegalArgumentException, IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "distinct block count");
		job.setJarByClass(CounterJob.class);
		job.setMapperClass(CounterJob.TokenizerMapper.class);
		job.setCombinerClass(CounterJob.IntSumReducer.class);
		job.setReducerClass(CounterJob.IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		job.waitForCompletion(true);
		// this counter is affected by the use of the combiner
		//		return job.getCounters().findCounter("vs", "distinct.block.count").getValue();
		return job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();
	}

}
