package edu.toronto.cs.sgbhadoop.hadoop220;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.TreeSet;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.MarkableIterator;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.toronto.cs.sgbhadoop.partition.PartitionNodesToRealNodes;
import edu.toronto.cs.sgbhadoop.sortedpackage.ConvertNtToInt;
import edu.toronto.cs.sgbhadoop.util.Timer;

/**
 *
 */
public class BisimulationGLFWBW {
	// input
	private static boolean flagDoCompact = true;

	//  interm
	private static boolean flagDoCompress = true;

	// debug
	static boolean flagDebug = false;
	static boolean flagDebugMapperIdentityWithSecSortKey = false;

	static final String SPLIT_CONSTANT = "\t| ";

	public static final String EMPTYSET_STRING = DigestUtils.md5Hex(DigestUtils.md5("0"));

	// const
	static final Text emptyText = new Text();

	public static class MapperIdentityWithSecSortKey extends Mapper<Object, Text, WritablePair, Text> {

		@Override
		protected void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper<Object, Text, WritablePair, Text>.Context context) throws IOException, InterruptedException {
			if (flagDebugMapperIdentityWithSecSortKey) {
				System.out.println(key + ":" + value);
			}
			// generate the secondary-sortable key
			final String parse[];
			final String keyMain;
			final String keySS;

			parse = value.toString().split(SPLIT_CONSTANT);
			final String s = parse[0];
			final String order = parse[1];
			final String p = parse[2];
			final String bid = parse[3];
			final String o = parse[4];

			// order, p, and bid are secondary sorted
			keySS = order + " " + p + " " + bid;

			// o or s (depending on order)
			Text tvalue = new Text(order + " " + p + " " + o);

			final WritablePair compKey1 = new WritablePair();
			compKey1.a = s;
			compKey1.b = keySS;
			context.write(compKey1, tvalue);
		}

		Timer rt;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			rt = new Timer("mapperidentitywithsecsort-timer-" + context.getTaskAttemptID(), true);
			System.out.println("starting:" + context.getTaskAttemptID());
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println(rt.stop());
		}
	}

	public static class FWBWBisimReducer extends Reducer<WritablePair, Text, Text, Text> {

		private MultipleOutputs<Text, Text> multipleOutputs;

		Timer rt;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<Text, Text>(context);
			rt = new Timer("reduce-timer-" + context.getTaskAttemptID(), true);
			System.out.println("starting:" + context.getTaskAttemptID());
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println(rt.stop());
			multipleOutputs.close();
		}

		@Override
		public void reduce(WritablePair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			MarkableIterator<Text> mitr = new MarkableIterator<Text>(values.iterator());
			mitr.mark();

			TreeSet<String> tsSet = new TreeSet<String>();

			while (mitr.hasNext()) {
				final String o = mitr.next().toString();
				final String s = context.getCurrentKey().a;
				String parse[] = context.getCurrentKey().b.split(SPLIT_CONSTANT);
				final String order = parse[0];
				final String p = parse[1];

				// might have to be processed as a byte array
				final String bid = parse[2];

				if (order.equals("0")) {
					tsSet.add(p + "*F*" + bid);
				} else if (order.equals("1")) {
					tsSet.add(p + "*B*" + bid);
				}
			}

			final String newbid = DigestUtils.md5Hex(DigestUtils.md5(tsSet.toString()));
			// compute the block id
			if (flagDebug) {
				// previous block id not stored in this stream so can't sysout
				System.out.println(context.getCurrentKey().a);
				System.out.println("tsSet:" + tsSet.toString());
				System.out.println("newbid:" + newbid);
			}

			// output the partition
			multipleOutputs.write(new Text(context.getCurrentKey().a), new Text(newbid), context.getConfiguration().get("iterpartitionoutput") + File.separatorChar + "nodes");

			// reset values iterator
			mitr.reset();

			// emit flipped stmts
			while (mitr.hasNext()) {
				final String val = mitr.next().toString();
				final String s = context.getCurrentKey().a;
				String parse[] = val.split(SPLIT_CONSTANT);
				final String order = parse[0];
				final String p = parse[1];
				final String o = parse[2];
				final String bid = parse[2];

				// write newbid to the graph
				if (order.equals("0")) {
					context.write(new Text(o + " " + 1 + " " + p + " " + newbid + " " + s), emptyText);
				} else if (order.equals("1")) {
					context.write(new Text(o + " " + 0 + " " + p + " " + newbid + " " + s), emptyText);
				}
			}
		}
	}

	public static class FBMapperApplyOrder extends Mapper<Object, Text, Text, Text> {

		private int outnum;

		Timer t;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			System.out.println("starting:" + context.getTaskAttemptID());
			t = new Timer("initializationmapper-" + context.getTaskAttemptID(), true);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("outnum final-" + context.getTaskAttemptID() + ":" + outnum);
			System.out.println(t.stop());
		}

		@Override
		protected void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			final String[] parse = value.toString().split(SPLIT_CONSTANT);

			final String s = parse[0];
			final String p = parse[1];
			final String o = parse[2];

			final String bid;
			if (parse.length > 3 && !parse[3].equals(".")) {
				bid = parse[3];
			} else {
				bid = EMPTYSET_STRING;
			}

			final Text textvalue = new Text();
			//			if (dir_F) {
			textvalue.set(s + " " + 0 + " " + p + " " + bid + " " + o);
			context.write(textvalue, emptyText);

			textvalue.set(o + " " + 1 + " " + p + " " + bid + " " + s);
			context.write(textvalue, emptyText);
			//			}
			/*
			 *not needed with markable iteration
			 */
			/*
			if (dir_B) {
				compKey1.a = s;
				compKey1.b = 2 + " " + p + " " + bid;
				textvalue.set(o);
				context.write(compKey1, textvalue);

				compKey1.a = o;
				compKey1.b = 3 + " " + p + " " + bid;
				textvalue.set(s);
				context.write(compKey1, textvalue);
			}
			*/

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

		String graphName = otherArgs[0];
		System.out.println("original-input:" + otherArgs[0]);
		String convertedinput = "";
		if (flagDoCompact) {
			convertedinput = otherArgs[0] + "-default-converted.gz";
			new ConvertNtToInt(otherArgs[0], convertedinput);
			graphName = convertedinput + File.separatorChar + "inted-clean.gz";
		}

		System.out.println("graphInput:" + graphName);
		final String inPath = graphName;
		final String outPath = otherArgs[1];

		int iteration = 0;

		// prep output path
		String iteroutpath = IterP(outPath, iteration); // iteration base output path
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(iteroutpath), true);

		String itergout = G(iteroutpath); // graph output path

		String itergoutprev = ""; // previous iteration's graph output path

		// no node or count output initially
		String iterpartitionoutput = ""; // node output path
		String itercout = ""; // count output path

		// run data initialization job
		System.out.println("init-in:" + inPath);
		System.out.println("init-out:" + itergout);
		Timer tinit = new Timer("init-full");
		System.out.println(tinit.start());
		applyCustomConfSettings(conf, iteration);
		Job job = createInitJob(conf, inPath, itergout);
		job.waitForCompletion(true);
		System.out.println(tinit.stop());

		long countold = 0;
		long countnew = 1;
		while (countold != countnew) {
			if (countold > countnew) {
				System.err.println("Old count is greater than new count!! stopping");
				return;
			}
			if (iteration >= 3) {
				Timer t4 = new Timer("iterdelete-" + iteration);
				System.out.println(t4.start());
				String olditerpath = IterP(outPath, iteration - 2);
				fs.delete(new Path(olditerpath), true);
				System.out.println(t4.stop());
			}

			// save previous iteration graph path and block count
			itergoutprev = itergout;
			countold = countnew;

			iteration++;

			Timer t1 = new Timer("iter-" + iteration);
			System.out.println(t1.start());

			iteroutpath = IterP(outPath, iteration);
			itergout = G(iteroutpath);
			iterpartitionoutput = N(iteroutpath);
			itercout = C(iteroutpath);

			applyCustomConfSettings(conf, iteration);

			// clear the output path; clears node, graph, and count
			fs.delete(new Path(iteroutpath), true);

			System.out.println("iteration:" + iteration);
			System.out.println("task-in:" + itergoutprev);
			System.out.println("task-out-graph:" + itergout);
			System.out.println("task-out-partition:" + iterpartitionoutput);
			Timer t2 = new Timer("task-" + iteration);
			System.out.println(t2.start());
			Job taskjob = createTaskJob(conf, itergoutprev, itergout, iterpartitionoutput, iteration);
			taskjob.waitForCompletion(true);
			System.out.println(t2.stop());

			Timer t3 = new Timer("count-" + iteration);
			System.out.println(t3.start());
			countnew = runCountJob(conf, iterpartitionoutput, itercout, iteration);
			System.out.println(t3.stop());
			System.err.println("counter:" + countnew);

			System.out.println(t1.stop());
		}
		if (flagDoCompact) {
			String nodesfile = convertedinput + File.separatorChar + "nodeids.gz";
			File f = new File(iterpartitionoutput);
			File[] listFiles = f.listFiles(new FileFilter() {
				@Override
				public boolean accept(File pathname) {
					return !pathname.getName().contains(".") || pathname.getName().endsWith(".gz");
				}
			});
			for (File f2 : listFiles) {
				PartitionNodesToRealNodes.main(new String[] { nodesfile, f2.toString(), f2.toString() + ".real" });
			}
		}

		System.out.println(t.stop());
		fs.close();
	}

	// return the iteration path
	private static String IterP(String path, int iteration) {
		return path + "-" + iteration;
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
	private static Job createTaskJob(Configuration conf, String inPath, String outPath, String iterpartitionoutput, int iteration) throws IOException, InterruptedException, ClassNotFoundException {
		conf.set("iterpartitionoutput", iterpartitionoutput);

		Job job = new Job(conf, "BisimFWBW-task-" + iteration);
		job.setJarByClass(BisimulationGLFWBW.class);

		job.setMapperClass(MapperIdentityWithSecSortKey.class);

		// Note: the combiner cannot be used since it cannot return distinct values across parallel tasks
		//job.setCombinerClass(notpossible);

		job.setMapOutputKeyClass(WritablePair.class);
		job.setMapOutputValueClass(Text.class);

		job.setSortComparatorClass(WritablePairKeyComparator.class);
		job.setPartitionerClass(WritablePairKeyPartitioner.class);
		job.setGroupingComparatorClass(WritablePairKeyGroupingComparator.class);

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
	private static Job createInitJob(Configuration conf, String inPath, String outPath) throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(conf, "BisimFWBW-initialization");
		job.setJarByClass(BisimulationGLFWBW.class);

		if (flagDoCompress) {
			FileOutputFormat.setCompressOutput(job, true);
			//	FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.BZip2Codec.class);
			//	FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.GzipCodec.class);
			//			FileOutputFormat.setOutputCompressorClass(job, nl.basjes.hadoop.io.compress.SplittableGzipCodec.class);
		}

		System.out.println("setting local max running maps:" + conf.getInt("vssp.nummaps", 1));
		LocalJobRunner.setLocalMaxRunningMaps(job, conf.getInt("vssp.nummaps", 1));

		job.setMapperClass(FBMapperApplyOrder.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(0); // no need to reduce here

		FileInputFormat.addInputPath(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		return job;
	}

	// customize the configuration going into the hadoop job
	private static Configuration applyCustomConfSettings(Configuration conf, int iteration) {
		conf.setBoolean("mapreduce.map.speculative", false);
		//		conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.DefaultCodec,nl.basjes.hadoop.io.compress.SplittableGzipCodec,org.apache.hadoop.io.compress.BZip2Codec");

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

		conf.set("mapreduce.task.io.sort.mb", "500");

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
