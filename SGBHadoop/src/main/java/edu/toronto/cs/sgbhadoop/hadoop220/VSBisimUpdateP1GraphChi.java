package edu.toronto.cs.sgbhadoop.hadoop220;

import java.io.File;
import java.io.IOException;
import java.util.TreeSet;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.toronto.cs.sgbhadoop.util.Timer;

public class VSBisimUpdateP1GraphChi implements Tool {

	// app config
	private static boolean flagDoCompress = false;

	public static final boolean flagDoFW = true;
	public static final boolean flagDoBW = false;

	//	public static final String vsBCPrefix = "http://bc.org/b/";

	public static String hash(String in) {
		return DigestUtils.md5Hex(in.getBytes());
	}

	public static class UpdateMapper extends Mapper<Object, Text, MyCompoundKey, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			final MyCompoundKey compKey1 = new MyCompoundKey();
			final MyCompoundKey compKey2 = new MyCompoundKey();
			final Text lineText = new Text();

			final String line = value.toString();
			// handle FW
			if (flagDoFW) {
				// line 1
				String parse[] = line.split(" ");

				if (parse.length == 3) {
					lineText.set(line + " " + hash("0"));
				} else {
					lineText.set(line);
				}
				// line 2
				compKey1.setS(parse[0]);
				compKey1.setOrder(0);
				//				WritablePair wp1 = new WritablePair("0", line);
				//				context.write(compKey1, wp1);
				context.write(compKey1, lineText);

				// line 3
				compKey2.setS(parse[2]);
				compKey2.setOrder(1);
				//				WritablePair wp2 = new WritablePair("1", line);
				//				context.write(compKey2, wp2);
				context.write(compKey2, lineText);
			}
		}
	}

	public static class UpdateReducer extends Reducer<MyCompoundKey, Text, Text, Text> {
		private MultipleOutputs<Text, Text> multipleOutputs;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<Text, Text>(context);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
		}

		@Override
		public void reduce(MyCompoundKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//			System.out.println("new key:" + key);
			// Handle FW
			if (flagDoFW) {

				String idkplusFW = null;

				// line 1
				// ordered set of id's on which to compute next id
				TreeSet<Sig> tsFW = new TreeSet<Sig>();

				boolean flagDoOnce = true;
				// line 2
				for (Text val : values) {
					//					System.out.println("key:" + context.getCurrentKey());
					if (context.getCurrentKey().order == 0) {

						String[] line = val.toString().split(" ");

						// line 4
						Sig s = new Sig(line[1].toString(), line[3].toString());
						if (!tsFW.contains(s)) {
							tsFW.add(s);
						}
						// line 5 end

					}

					if (context.getCurrentKey().order == 1) {
						if (flagDoOnce) {
							flagDoOnce = false;
							if (tsFW.size() == 0) {
								idkplusFW = hash("0");
							} else {
								idkplusFW = hash(tsFW.toString());
							}
						}
						if (idkplusFW == null) {
							System.err.println("Cannot have null sig here");
						}

						String[] line = val.toString().split(" ");

						// line 10
						String newLine = line[0] + " " + line[1] + " " + line[2] + " " + idkplusFW;
						//						System.out.println("newline:" + newLine);
						//				context.write(new Text(""), new Text(newLine));
						multipleOutputs.write(new Text(""), new Text(newLine), context.getConfiguration().get("vs.outbasepath") + File.separatorChar + "graph" + File.separatorChar + "graph");
						// line 11 end
					}

				}

				if (flagDoOnce) {
					if (tsFW.size() == 0) {
						idkplusFW = hash("0");
					} else {
						idkplusFW = hash(tsFW.toString());
					}
				}
				// line 12, 13
				Text k = new Text();
				Text v = new Text();
				try {
					k.set(key.s);
					v.set(idkplusFW);
				} catch (Exception e) {
					e.printStackTrace();
				}
				//				System.out.println("k:" + k + ", v:" + v);

				// place s first to order by s
				//				multipleOutputs.write(k, v, context.getConfiguration().get("vs.outbasepath") + File.separatorChar + "nodes" + File.separatorChar + "nodes");

				// place block first to order by block id
				multipleOutputs.write(v, k, context.getConfiguration().get("vs.outbasepath") + File.separatorChar + "nodes" + File.separatorChar + "nodes");
			}

		}
	}

	public static void main(String[] args) throws Exception {

		ToolRunner.run(new VSBisimUpdateP1GraphChi(), args);
	}

	private Configuration conf;

	private void mymain(String[] args) throws Exception {
		Logger.getRootLogger().setLevel(Level.ERROR);

		System.out.println("Starting..");
		Timer t = new Timer("fulljob", true);
		conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: vsbisimupdatep1 <in> <out>");
			System.exit(2);
		}

		String inPath = otherArgs[0];
		String outPath = otherArgs[1];

		int fnum = 0;
		Timer t1 = new Timer("init", true);
		Job job = createJob(inPath, outPath + "-" + fnum);
		job.waitForCompletion(true);
		System.out.println(t1.stop());

		//		System.exit(job.waitForCompletion(true) ? 0 : 1);

		long lastrecordcount = runCountJob(outPath + "-" + fnum + File.separatorChar + "nodes", outPath + "-" + fnum + File.separatorChar + "count");

		System.out.println("counter:" + lastrecordcount);
		while (true) {
			String f1 = outPath + "-" + fnum++ + File.separatorChar + "graph";
			String f2 = outPath + "-" + fnum;

			Timer t2 = new Timer("iter", true);
			Job jobInner = createJob(f1, f2);
			jobInner.waitForCompletion(true);
			System.out.println(t2.stop());

			//			long reduceoutputrecords = jobInner.getCounters().getGroup("org.apache.hadoop.mapreduce.TaskCounter").findCounter("REDUCE_OUTPUT_RECORDS").getValue();
			//			long reduceoutputrecords = jobInner.getCounters().getGroup("org.apache.hadoop.mapreduce.lib.output.MultipleOutputs").findCounter("nullgraph\\graph").getValue();
			Timer t3 = new Timer("count", true);
			long reduceoutputrecords = runCountJob(f2 + File.separatorChar + "nodes", f2 + File.separatorChar + "count");
			System.out.println(t3.stop());
			System.out.println("counter:" + reduceoutputrecords);
			if (reduceoutputrecords == lastrecordcount) {
				break;
			}
			lastrecordcount = reduceoutputrecords;
		}

		System.out.println(t.stop());

	}

	private static Job createJob(String inPath, String outPath) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "VSBisimUpdateJob");
		FileSystem fs = FileSystem.get(conf);

		job.setJarByClass(VSBisimUpdateP1GraphChi.class);

		job.setMapperClass(UpdateMapper.class);
		job.setMapOutputKeyClass(MyCompoundKey.class);
		job.setMapOutputValueClass(Text.class);

		// Note: the combiner cannot be used since it cannot return distinct values across parallel tasks
		//job.setCombinerClass(UpdateReducer.class);

		job.setSortComparatorClass(MyCompoundKeyComparator.class);
		job.setPartitionerClass(MyCompoundKeyPartitioner.class);
		job.setGroupingComparatorClass(MyCompoundKeyGroupingComparator.class);

		job.setReducerClass(UpdateReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		if (flagDoCompress) { //these work
			FileOutputFormat.setCompressOutput(job, true);
			//			FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.BZip2Codec.class);
			FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.GzipCodec.class);
			conf.set("mapreduce.map.output.compress", "true");
			conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
		}

		boolean flagDoOptions = false; //not working
		if (flagDoOptions) {
			conf.set("mapred.max.split.size", "1000");
			conf.set("mapreduce.task.io.sort.mb", "1000");
			conf.set("io.file.buffer.size", "13107200");
			conf.set("mapreduce.map.sort.spill.percent", "0.33");
			conf.set("mapred.job.shuffle.input.buffer.percent", "0.33");
			conf.set("mapred.job.reduce.input.buffer.percent", "0.33");
		}

		//		conf.set("mapreduce.local.map.tasks.maximum", "4");
		//		conf.set("mapreduce.tasktracker.map.tasks.maximum", "3");

		job.getConfiguration().set("vs.outbasepath", outPath);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		MultipleOutputs.setCountersEnabled(job, true);
		//		MultipleOutputs.addNamedOutput(job, "graph", TextOutputFormat.class, Text.class, Text.class);
		//		MultipleOutputs.addNamedOutput(job, "nodes", TextOutputFormat.class, Text.class, Text.class);

		// true stands for recursively deleting the folder you gave
		fs.delete(new Path(outPath), true);

		return job;
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

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		mymain(args);
		return 0;
	}

}
