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

import edu.toronto.cs.sgbhadoop.util.GeneralUtil;
import edu.toronto.cs.sgbhadoop.util.Timer;

/**
 * This class works.
 *
 */
public class BisimFWBWMPC {
	// init - a map-only job generates 2 statements for each input stmt, spo and s-po
	// task - map emits input stmt as is, reduce processes fwbw hash on first read, generates flipped stmts on second read

	// flag to skip lines that do not have valid IRIs
	// should be enabled for real datasets, disabled for example datasets
	public static final boolean flagValidateIRIs = false;

	// whether intermediate output should be compressed
	private static boolean flagDoCompress = true;

	private static boolean flagDebug = true;

	static final Text emptyText = new Text();

	static final boolean flagshowtimers = false;

	// input is lines of records that are output as compound keys for sorting
	public static class FWBWBisimMapper extends Mapper<Object, Text, MyCompoundKey, Text> {

		private static final String SPLIT_CONSTANT = " ";

		//		private static final String SPLIT_CONSTANT = "\t| ";

		@Override
		protected void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
			final MyCompoundKey compKey1 = new MyCompoundKey();
			String parse[];
			String scompoundkey;
			String p;
			String o;
			String bid;
			String keyparse;
			char lchar;
			final Text emitline = new Text();

			final StringBuilder sb = new StringBuilder();

			sb.setLength(0);
			parse = value.toString().split(SPLIT_CONSTANT);
			scompoundkey = parse[0];
			p = parse[1];
			bid = parse[2];
			o = parse[3];

			keyparse = scompoundkey.substring(0, scompoundkey.length() - 1);
			lchar = scompoundkey.charAt(scompoundkey.length() - 1);

			compKey1.setS(keyparse);
			//			try {
			compKey1.setOrder(Integer.parseInt("" + lchar));
			//			} catch (Exception e) {
			//				e.printStackTrace();
			//				System.out.println(value);
			//			}
			sb.append(p);
			sb.append(" ");
			sb.append(bid);
			sb.append(" ");
			sb.append(o);
			// p + " " + bid + " " + o
			emitline.set(sb.toString());
			context.write(compKey1, emitline);
		}
	}

	// requires keys with a common prefix (aside from 0 and 1) 
	public static class FWBWBisimReducer extends Reducer<MyCompoundKey, Text, Text, Text> {

		private MultipleOutputs<Text, Text> multipleOutputs;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<Text, Text>(context);
			System.out.println("starting:" + context.getTaskAttemptID());
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
		}

		// ordered set of id's on which to compute next id
		final TreeSet<Sig> tsFW = new TreeSet<Sig>();
		final Text nodek = new Text();
		final Text nodev = new Text();
		String bidFWBW;

		@Override
		public void reduce(MyCompoundKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			//		final Text currval;

			String parse[];
			String p, o, bid;

			final Text graphk = new Text();

			boolean flagDoOnce = true;

			final StringBuilder sb = new StringBuilder();
			final StringBuilder sb1 = new StringBuilder();

			tsFW.clear();
			sb.setLength(0);

			flagDoOnce = true;

			final Timer t1 = new Timer("values0123iterator-" + context.getTaskAttemptID());
			if (flagshowtimers)
				System.err.println(t1.start());

			for (Text currval : values) {

				parse = currval.toString().split("\t| ");
				p = parse[0];
				bid = parse[1];
				o = parse[2];

				if (key.order == 0) {
					final Sig s = new Sig(p, bid);
					if (!tsFW.contains(s)) {
						tsFW.add(s);
					}
				} else if (key.order == 1) {
					final Sig s = new Sig("-" + p, bid);
					if (!tsFW.contains(s)) {
						tsFW.add(s);
					}
				}

				if (key.order > 1 && flagDoOnce) {
					flagDoOnce = false;
					writeNodeOut(key, context);
				}

				if (key.order > 1) {
					sb.setLength(0);
					sb.append(" ");
					sb.append(p);
					sb.append(" ");
					sb.append(bidFWBW);
					sb.append(" ");
					sb.append(key.s);
					//					innerstring = " " + p + " " + bidFWBW + " " + key.s;

					final Timer t2 = new Timer("reducecontextwrite-" + context.getTaskAttemptID());
					if (flagshowtimers)
						System.err.println(t2.start());

					if (key.order == 2) {
						sb1.setLength(0);
						sb1.append(o);
						sb1.append("1");
						sb1.append(sb);
						//						graphemitline = o + "1" + innerstring;
						//						graphk.set(graphemitline);
						graphk.set(sb1.toString());
						context.write(graphk, emptyText);
						sb1.setLength(0);
						sb1.append(o);
						sb1.append("3");
						sb1.append(sb);
						graphk.set(sb1.toString());
						//						graphemitline = o + "3" + innerstring;
						//						graphk.set(graphemitline);
						context.write(graphk, emptyText);
					} else if (key.order == 3) {
						sb1.setLength(0);
						sb1.append(o);
						sb1.append("0");
						sb1.append(sb);
						graphk.set(sb1.toString());
						//						graphemitline = o + "0" + innerstring;
						//						graphk.set(graphemitline);
						context.write(graphk, emptyText);
						sb1.setLength(0);
						sb1.append(o);
						sb1.append("2");
						sb1.append(sb);
						graphk.set(sb1.toString());
						//						graphemitline = o + "2" + innerstring;
						//						graphk.set(graphemitline);
						context.write(graphk, emptyText);
					}

					if (flagshowtimers)
						System.err.println(t2.stop());

					//					graphk.set(graphemitline);
					//					context.write(graphk, emptyText);
				}

			}
			if (flagshowtimers)
				System.err.println(t1.stop());

			if (flagDoOnce) {
				writeNodeOut(key, context);
			}
		}

		private void writeNodeOut(MyCompoundKey key, Context context) throws IOException, InterruptedException {
			final Timer t = new Timer("writenodeout-" + context.getTaskAttemptID());
			if (flagshowtimers)
				System.err.println(t.start());

			// compute FWBW blockid
			bidFWBW = GeneralUtil.getMD5(tsFW.toString());
			if (flagDebug) {
				System.out.println("currentKey:" + key.s);
				System.out.println("ts:" + tsFW);
				System.out.println("bidfwbw:" + bidFWBW);
			}
			// output node record
			nodek.set(key.s);
			nodev.set(bidFWBW);

			if (flagshowtimers)
				System.err.println(t.stop());

			final Timer t3 = new Timer("multipleOutputs-node-" + context.getTaskAttemptID());
			if (flagshowtimers)
				System.err.println(t3.start());
			multipleOutputs.write(nodek, nodev, context.getConfiguration().get("iternout") + File.separatorChar + "nodes");
			if (flagshowtimers)
				System.err.println(t3.stop());
		}

		/*
		 * 
		//		MarkableIterator<Text> mitr = null;
		//		String innerstring;
		//		private String graphemitline;
		//		@Override
		public void reduce1(MyCompoundKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			mitr = new MarkableIterator<Text>(values.iterator());

			tsFW.clear();

			// set the mark at the beginning of the stream
			mitr.mark();

			while (mitr.hasNext()) {
				currval = mitr.next();

				parse = currval.toString().split("\t| ");
				p = parse[0];
				bid = parse[1];
				//				o = parse[2];

				if (key.order == 0) {
					Sig s = new Sig(p, bid);
					if (!tsFW.contains(s)) {
						tsFW.add(s);
					}
				} else if (key.order == 1) {
					Sig s = new Sig("-" + p, bid);
					if (!tsFW.contains(s)) {
						tsFW.add(s);
					}
				}

			}

			// reset values iterator
			mitr.reset();

			writeNodeOut(key, context);

			// emit flipped stmts
			while (mitr.hasNext()) {
				currval = mitr.next();

				parse = currval.toString().split("\t| ");
				p = parse[0];
				bid = parse[1];
				o = parse[2];

				innerstring = " " + p + " " + bidFWBW + " " + key.s;

				if (key.order == 0) {
					graphemitline = o + "1" + innerstring;
				} else if (key.order == 1) {
					graphemitline = o + "0" + innerstring;
				}

				graphk.set(graphemitline);
				context.write(graphk, emptyText);
			}
		}
		*/
	}

	/**
	 * Mapper used for initializing a dataset for iterative computation
	 *
	 */
	public static class FWBWMapperInitialization extends Mapper<Object, Text, Text, Text> {
		private static final String _3 = "3";

		private static final String _1 = "1";

		private static final String _2 = "2";

		private static final String _0 = "0";

		private static final String SPACECHAR = " ";

		private MultipleOutputs<Text, Text> multipleOutputs;

		String[] parse;
		private int outnum;

		Timer t;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			System.out.println("starting:" + context.getTaskAttemptID());
			t = new Timer("initializationmapper-" + context.getTaskAttemptID(), true);
			multipleOutputs = new MultipleOutputs<Text, Text>(context);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
			System.out.println("outnum final-" + context.getTaskAttemptID() + ":" + outnum);
			System.out.println(t.stop());
		}

		final Text graphline = new Text();
		final Text zeroline = new Text();
		final Text oneline = new Text();

		String s;
		String p;
		String o;
		String bid;
		String lastpart;

		final StringBuilder sb = new StringBuilder();

		@Override
		protected void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
			parse = value.toString().split("\t| ");

			if (parse.length != 4 && parse.length != 5) {
				//								logger.finest("skipping line (not at least a triple):" + line);
				// require at least a triple
				return;
			}

			// check that it is a valid nt statement
			if (parse[parse.length - 1].charAt(0) != '.') {
				return;
			}

			s = parse[0];
			p = parse[1];
			o = parse[2];

			if (o.charAt(0) != '<' || s.charAt(0) != '<' || p.charAt(0) != '<') {
				// ignore statements with literals, and check that it is somewhat well-formed
				return;
			}

			if (parse.length == 4 && parse[3].equals(".")) {
				// assign an initial block identifier
				bid = _0;
			} else {
				bid = parse[3];
			}

			//			sb.setLength(0);
			//			sb.append(SPACECHAR);
			//			sb.append(p);
			//			sb.append(SPACECHAR);
			//			sb.append(bid);
			//			sb.append(SPACECHAR);
			lastpart = SPACECHAR + p + SPACECHAR + bid + SPACECHAR;

			// store copy of accepted stmts in separate graph file to upload to rdf store
			graphline.set(s + lastpart + o + " .");
			multipleOutputs.write(graphline, emptyText, context.getConfiguration().get("iterinstout") + File.separatorChar + "instance");
			//			multipleOutputs.write("instance", graphline, emptyText, INSTANCE_OUTPUT_PATH);

			zeroline.set(s + _0 + lastpart + o);
			context.write(zeroline, emptyText);

			zeroline.set(s + _2 + lastpart + o);
			context.write(zeroline, emptyText);

			oneline.set(o + _1 + lastpart + s);
			context.write(oneline, emptyText);

			oneline.set(o + _3 + lastpart + s);
			context.write(oneline, emptyText);

			outnum++;
			if (outnum % 1000000 == 0) {
				System.out.println("timer:" + t);
				System.out.println("num of lines:" + outnum);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Logger.getRootLogger().setLevel(Level.INFO);

		Timer t = new Timer("fulljob");
		System.out.println(t.start());
		Configuration conf = new Configuration();

		if (System.getProperty("vssp.nummaps") == null) {
			System.setProperty("vssp.nummaps", "4");
		}
		System.out.println("vssp.nummaps:" + System.getProperty("vssp.nummaps"));
		conf.setInt("vssp.nummaps", Integer.parseInt(System.getProperty("vssp.nummaps")));

		System.out.println("conf.tmp:" + System.getProperty("hadoop.tmp.dir"));
		conf.set("hadoop.tmp.dir", System.getProperty("hadoop.tmp.dir"));

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: BisimFWBW <in> <out>");
			System.exit(2);
		}

		String inPath = otherArgs[0];
		String outPath = otherArgs[1];

		FileSystem fs = FileSystem.get(conf);
		int iteration = 0;
		String iteroutpath = ""; // iteration base output path
		String itergout = ""; // graph output path
		String itergoutprev = ""; // previous iteration's graph output path
		String iternout = ""; // node output path
		String itercout = ""; // count output path
		String iterinstout = ""; // instance output path

		iteroutpath = IP(outPath, iteration);
		itergout = G(iteroutpath);
		iterinstout = Inst(iteroutpath);
		// no node or count output initially

		// prep output path
		fs.delete(new Path(iteroutpath), true);

		// run data initialization job
		Timer tinit = new Timer("init");
		System.out.println(tinit.start());
		applyCustomConfSettings(conf, iteration);
		Job job = createInitJob(conf, inPath, itergout, iterinstout);
		job.waitForCompletion(true);
		System.out.println(tinit.stop());

		long countold = 0;
		long countnew = 1;
		while (countold != countnew) {
			if (iteration >= 3) {
				Timer t4 = new Timer("iterdelete-" + iteration);
				System.out.println(t4.start());
				String olditerpath = IP(outPath, iteration - 2);
				fs.delete(new Path(olditerpath), true);
				System.out.println(t4.stop());
			}

			// save previous iteration graph path
			itergoutprev = itergout;

			// save previous block count
			countold = countnew;

			iteration++;

			Timer t1 = new Timer("iter-" + iteration);
			System.out.println(t1.start());

			iteroutpath = IP(outPath, iteration);
			itergout = G(iteroutpath);
			iternout = N(iteroutpath);
			itercout = C(iteroutpath);

			applyCustomConfSettings(conf, iteration);

			// clear the output path; clears node, graph, and count
			fs.delete(new Path(iteroutpath), true);

			// TODO: perform task job
			Timer t2 = new Timer("task-" + iteration);
			System.out.println(t2.start());
			Job taskjob = createTaskJob(conf, itergoutprev, itergout, iternout, iteration);
			taskjob.waitForCompletion(true);
			System.out.println(t2.stop());

			Timer t3 = new Timer("count-" + iteration);
			System.out.println(t3.start());
			countnew = runCountJob(conf, iternout, itercout, iteration);
			System.out.println(t3.stop());
			System.err.println("counter:" + countnew);

			System.out.println(t1.stop());
		}

		System.out.println(t.stop());
		fs.close();
	}

	// return the iteration path
	private static String IP(String path, int iteration) {
		return path + "-" + iteration;
	}

	// return the instance path
	private static String Inst(String path) {
		return path + File.separatorChar + "instance";
	}

	// return the graph path
	private static String G(String path) {
		return path + File.separatorChar + "graph";
	}

	// return the node path
	private static String N(String path) {
		return path + File.separatorChar + "nodes";
	}

	private static String C(String path) {
		return path + File.separatorChar + "count";
	}

	// input from graph statements of previous iteration
	// processes and emits graph statements for next iteration
	// stores node block ids for counting in multiple output (path internout via conf) 
	private static Job createTaskJob(Configuration conf, String inPath, String outPath, String iternout, int iteration) throws IOException, InterruptedException, ClassNotFoundException {
		conf.set("iternout", iternout);

		Job job = new Job(conf, "BisimFWBW-task-" + iteration);
		job.setJarByClass(BisimFWBWMPC.class);

		job.setMapperClass(FWBWBisimMapper.class);

		// Note: the combiner cannot be used since it cannot return distinct values across parallel tasks
		//job.setCombinerClass(notpossible);

		// allow for secondary sort on 0 or 1 tail
		job.setMapOutputKeyClass(MyCompoundKey.class);
		job.setMapOutputValueClass(Text.class);

		job.setSortComparatorClass(MyCompoundKeyComparator.class);
		job.setPartitionerClass(MyCompoundKeyPartitioner.class);
		job.setGroupingComparatorClass(MyCompoundKeyGroupingComparator.class);

		job.setReducerClass(FWBWBisimReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//		conf.set("mapred.local.map.tasks.maximum", "4");
		System.out.println("setting local max running maps:" + conf.getInt("vssp.nummaps", 1));
		LocalJobRunner.setLocalMaxRunningMaps(job, conf.getInt("vssp.nummaps", 1));
		//		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(inPath));
		//		FileInputFormat.addInputPath(job, new Path(conf.get("vssp.noderestorepath") + File.separatorChar + "nodes" + "*.gz")); // + File.separatorChar + "nodes"
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		if (flagDoCompress) {
			FileOutputFormat.setCompressOutput(job, true);
			//			FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.BZip2Codec.class);
			FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.GzipCodec.class);
			//			FileOutputFormat.setOutputCompressorClass(job, nl.basjes.hadoop.io.compress.SplittableGzipCodec.class);
		}

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		MultipleOutputs.addNamedOutput(job, "node", TextOutputFormat.class, Text.class, Text.class);

		//		MultipleOutputs.setCountersEnabled(job, true);

		return job;
	}

	// construct the job for the initialization of data
	private static Job createInitJob(Configuration conf, String inPath, String outPath, String iterinstout) throws IOException, InterruptedException, ClassNotFoundException {
		conf.set("iterinstout", iterinstout);

		Job job = new Job(conf, "BisimFWBW-initialization");
		job.setJarByClass(BisimFWBWMPC.class);

		if (flagDoCompress) {
			FileOutputFormat.setCompressOutput(job, true);
			//	FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.BZip2Codec.class);
			//	FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.GzipCodec.class);
			//			FileOutputFormat.setOutputCompressorClass(job, nl.basjes.hadoop.io.compress.SplittableGzipCodec.class);
		}

		System.out.println("setting local max running maps:" + conf.getInt("vssp.nummaps", 1));
		LocalJobRunner.setLocalMaxRunningMaps(job, conf.getInt("vssp.nummaps", 1));

		job.setMapperClass(FWBWMapperInitialization.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(0); // no need to reduce here

		FileInputFormat.addInputPath(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//		MultipleOutputs.addNamedOutput(job, "instance", TextOutputFormat.class, Text.class, Text.class);

		return job;
	}

	// customize the configuration going into the hadoop job
	private static Configuration applyCustomConfSettings(Configuration conf, int iteration) {
		conf.setBoolean("mapreduce.map.speculative", false);
		conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.DefaultCodec,nl.basjes.hadoop.io.compress.SplittableGzipCodec,org.apache.hadoop.io.compress.BZip2Codec");

		// buffer for mark-reset functionality, anything larger than this is cached to disk
		// configuration handled in class org.apache.hadoop.mapred.BackupStore
		conf.set("mapreduce.reduce.markreset.buffer.size", 10000000 + "");

		// TODO: have some way of detecing appropriate split size for maps/threads

		// lmdb - uncompressed - for 8 threads
		//		conf.set("mapreduce.input.fileinputformat.split.minsize", 50000000 + "");
		//		conf.set("mapreduce.input.fileinputformat.split.maxsize", 50000000 + "");

		// lmdb - uncompressed - 4 threads
		//		conf.set("mapreduce.input.fileinputformat.split.minsize", "200000000");
		//		conf.set("mapreduce.input.fileinputformat.split.maxsize", "300000000");

		// lmdb - compressed
		//		conf.set("mapreduce.input.fileinputformat.split.minsize", "6000000");
		//		conf.set("mapreduce.input.fileinputformat.split.maxsize", "6000000");

		if (flagDoCompress) {
			conf.set("mapreduce.input.fileinputformat.split.minsize", "3000000");
			conf.set("mapreduce.input.fileinputformat.split.maxsize", "3000000");
		} else {
			conf.set("mapreduce.input.fileinputformat.split.minsize", "60000000");
			conf.set("mapreduce.input.fileinputformat.split.maxsize", "60000000");
		}

		// dbpedia
		//		conf.set("mapreduce.input.fileinputformat.split.minsize", "900000000");
		//		conf.set("mapreduce.input.fileinputformat.split.maxsize", "1000000000");

		conf.set("mapreduce.task.io.sort.mb", "200");

		if (flagDoCompress) {
			conf.setBoolean("mapred.compress.map.output", true);
			conf.setBoolean("mapreduce.map.output.compress", true);
		}
		return conf;
	}

	private static long runCountJob(Configuration conf, String inPath, String outPath, int iteration) throws IllegalArgumentException, IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(conf, "CountDistinctBlock-" + iteration);
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
