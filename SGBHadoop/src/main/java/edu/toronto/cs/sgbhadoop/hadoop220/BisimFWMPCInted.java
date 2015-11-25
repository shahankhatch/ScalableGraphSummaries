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
import org.apache.hadoop.mapreduce.MarkableIterator;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.toronto.cs.sgbhadoop.util.GeneralUtil;
import edu.toronto.cs.sgbhadoop.util.Timer;
import java.util.HashSet;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * FW bisimulation verified against GraphChi versions. Uses key order to ensure that node blockid is pulled before its edges
 */
public class BisimFWMPCInted {
	// init - a map-only job generates 2 statements for each input stmt, spo and s-po
	// task - map emits input stmt as is, reduce processes fwbw hash on first read, generates flipped stmts on second read

	// whether intermediate output should be compressed
	private static boolean flagDoCompress = true;
	
	private static boolean flagDebug = false;
	
	static final Text emptyText = new Text();
	
	static int iteration = 0;
	
	static final boolean flagPartitionGraphOutput = true;
	// implemented flag
	// implemented creation of named output (is it necessary?)
	// looked at api for writing named otuput 
	// code to write graph to named output is commented out (almost ready, just specify path)
	// However, it separates into folders so need to add respective folder inputs to job
	// current todo:
	// enable write to output partition folders
	// add folder inputs to job
	// test

	//	static int reduceVersion = 1; // markable iterator version, doesn't work
	// input is lines of records that are output as compound keys for sorting
	public static class FWBWBisimMapper extends Mapper<Object, Text, MyCompoundKey, Text> {
		
		private static final String SPLIT_CONSTANT = "\t| ";

		//		private static final String SPLIT_CONSTANT = "\t| ";
		int linenum = 0;
//				int maxlines = 100;
		int maxlines = Integer.MAX_VALUE;
		
		@Override
		protected void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
			final MyCompoundKey compKey1 = new MyCompoundKey();
			String parse[];
			String s;
			String p;
			String o;
			String bid;
			final Text emitline = new Text();
			
			if (iteration == 0 && linenum++ >= maxlines) {
				return;
			}

			//			if (iteration == 1) {
			//				System.out.println("");
			//			}
			parse = value.toString().split(SPLIT_CONSTANT);
			try {
				if (parse.length == 4) {
					s = parse[0];
					p = parse[1];
					bid = parse[2];
					o = parse[3];
				} else if (parse.length == 1) {
					String parse2[] = parse[0].split("\t| ");
//					String kk = parse2[0].substring(0, parse2[0].length() - 1);
					//					String or = parse2[0].substring(parse2[0].length() - 1);
					//					context.write(new MyCompoundKey(kk, 0), new Text(parse2[1]));
					return;
				} else {
					
					s = parse[0];
					p = parse[1];
					bid = "0";
					o = parse[2];
				}
				
				compKey1.setS(s);
				compKey1.setOrder(1);
				emitline.set(s + " " + p + " " + bid + " " + o);
				context.write(compKey1, emitline);
				
				compKey1.setS(o);
				compKey1.setOrder(2);
				emitline.set(s + " " + p + " " + bid + " " + o);
				context.write(compKey1, emitline);
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// requires keys with a common prefix (aside from 0 and 1) 
	public static class FWBWBisimReducer extends Reducer<MyCompoundKey, Text, Text, NullWritable> {
		
		int numpartitions = 6;
		private MultipleOutputs<Text, NullWritable> multipleOutputs;
		String reduceriteration;
		
		int reduceMaxCount = 30000; // max num of records per reduce output file

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<>(context);
			System.out.println("starting:" + context.getTaskAttemptID());
			reduceriteration = context.getConfiguration().get("iternout");
			numpartitions = (int) (context.getConfiguration().getInt("vssp.partitions",numpartitions));
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
		}

		// ordered set of id's on which to compute next id
		final Text nodek = new Text();
//		final Text nodev = new Text();
		
		@Override
		public void reduce(MyCompoundKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String parse[];
			String s, p, o, bid;
			
			final HashSet<Sig> tsFWSet = new HashSet<>();
			String bidFWBW = null;
			
			final Text graphk = new Text();
			
			boolean flagDoOnce = true;
			
			String currbid;
			for (Text currval : values) {
				
				parse = currval.toString().split("\t| ");
				
				if (key.order == 0) {
					currbid = parse[0];
					tsFWSet.add(new Sig("-99999", currbid));
					continue;
				}
				
				s = parse[0];
				p = parse[1];
				bid = parse[2];
				o = parse[3];
				
				if (key.order == 1) {
					
					final Sig sighere = new Sig(p, bid);
					if (!tsFWSet.contains(sighere)) {
						tsFWSet.add(sighere);
					}
					continue;
				}
				
				if (flagDoOnce) {
					flagDoOnce = false;
					bidFWBW = writeNodeOut(key, reduceriteration, tsFWSet, context.getTaskAttemptID().toString());
				}
				
				if (key.order == 2) {
					writeGraphRecord(graphk, s, p, bidFWBW, o, context);
				}
			}
			
			if (flagDoOnce) {
				writeNodeOut(key, reduceriteration, tsFWSet, context.getTaskAttemptID().toString());
			}
		}
		
		final NullWritable nw = NullWritable.get();
		
		public void writeGraphRecord(final Text graphk, String s, String p, String bidFWBW, String o, Context context) throws InterruptedException, IOException {
//			final Text emptyText = new Text();

			graphk.set(s + " " + p + " " + bidFWBW + " " + o);
			if (flagPartitionGraphOutput) {
				int partitionid = MyCompoundKeyPartitioner.getPartitionStatic(s, numpartitions);
//				System.out.println("partitionid:"+partitionid);
				multipleOutputs.write(graphk, nw, iteration + File.separatorChar + "graph" + partitionid);
			} else {
				multipleOutputs.write(graphk, nw, iteration + File.separatorChar + "graph");

				// original line of code
//				context.write(graphk, emptyText);
			}
		}
		
		private String writeNodeOut(MyCompoundKey key, String iteration, HashSet<Sig> tsFWSet, String id) throws IOException, InterruptedException {
			// compute FWBW blockid
			final TreeSet<Sig> tsFW = new TreeSet<>(tsFWSet);
			
			String bidFWBW = GeneralUtil.getMD5(tsFW.toString());
			if (flagDebug) {
				System.out.println("currentKey:" + key.s);
				System.out.println("ts:" + tsFW);
				System.out.println("bidfwbw:" + bidFWBW);
			}
			// output node record
//			nodek.set(key.s + "0");
//			nodev.set(bidFWBW);
			
			nodek.set(key.s + "\t" + bidFWBW);
//			nodev.set(bidFWBW);

			int partitionid = MyCompoundKeyPartitioner.getPartitionStatic(key.s, numpartitions);
			if (flagPartitionGraphOutput) {
				multipleOutputs.write(nodek, nw, iteration + File.separatorChar + "nodes-"+id+"-" + partitionid);
			} else {
				multipleOutputs.write(nodek, nw, iteration + File.separatorChar + "nodes-"+id);
			}
			
			return bidFWBW;
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Logger.getRootLogger().setLevel(Level.INFO);
		
		System.out.println("args:");
		for (String a : args) {
			System.out.println(a);
		}
		
		Timer t = new Timer("fulljob");
		System.out.println(t.start());
		Configuration conf = new Configuration();
		
		if (System.getProperty("vssp.nummaps") == null) {
			System.setProperty("vssp.nummaps", "4");
		}
		System.out.println("vssp.nummaps:" + System.getProperty("vssp.nummaps"));
		conf.setInt("vssp.nummaps", Integer.parseInt(System.getProperty("vssp.nummaps")));
		
		System.out.println("conf.tmp:" + System.getProperty("hadoop.tmp.dir",""));
		conf.set("hadoop.tmp.dir", System.getProperty("hadoop.tmp.dir",""));
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: BisimFWBW <in> <out>");
			System.exit(2);
		}
		
		String inPath = otherArgs[0];
		String outPath = otherArgs[1];
		
		FileSystem fs = FileSystem.get(conf);
		String iteroutpath = ""; // iteration base output path
		String itergout = ""; // graph output path
		String itergoutprev = ""; // previous iteration's graph output path
		String iternoutprev = ""; // previous iteration's node output path
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
		//		Timer tinit = new Timer("init");
		//		System.out.println(tinit.start());
		//		applyCustomConfSettings(conf, iteration);
		//				Job job = createInitJob(conf, inPath, itergout, iterinstout);
		//		job.waitForCompletion(true);
		//		System.out.println(tinit.stop());
		long countold = 0;
		long countnew = 1;
		while (countold != countnew) {
			if (iteration >= 2) {
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
			
			Timer t1 = new Timer("iter-" + iteration);
			System.out.println(t1.start());
			
			String iteroutprev = "";
			if (iteration > 0) {
				iteroutprev = IP(outPath, iteration - 1);
			} else {
				iteroutprev = IP(outPath, iteration);
			}
			iternoutprev = N(iteroutprev);
			
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
			Job taskjob = createTaskJob(conf, itergoutprev, iternoutprev, itergout, iternout, iteration, inPath);
			taskjob.waitForCompletion(true);
			System.out.println(t2.stop());
			
			Timer t3 = new Timer("count-" + iteration);
			System.out.println(t3.start());
			countnew = runCountJob(conf, iternout, itercout, iteration);
			System.out.println(t3.stop());
			System.err.println("counter:" + countnew);
			
			System.out.println(t1.stop());
			iteration++;
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
	private static Job createTaskJob(Configuration conf, String itergoutprev, String iternoutprev, String outPath, String iternout, int iteration, String inPath) throws IOException,
			  InterruptedException, ClassNotFoundException {
		conf.set("iternout", iternout);
		
		Job job = new Job(conf, "BisimFWBW-task-" + iteration);
		job.setJarByClass(BisimFWMPCInted.class);
		
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
//		job.setOutputValueClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		//		conf.set("mapred.local.map.tasks.maximum", "4");
		//		System.out.println("setting local max running maps:" + conf.getInt("vssp.nummaps", 1));
		//		job.setNumReduceTasks(1);
		if (System.getProperty("vssp.nummaps") == null) {
			System.setProperty("vssp.nummaps", "4");
		}
		System.out.println("vssp.nummaps:" + System.getProperty("vssp.nummaps"));
		conf.setInt("vssp.nummaps", Integer.parseInt(System.getProperty("vssp.nummaps")));
		LocalJobRunner.setLocalMaxRunningMaps(job, conf.getInt("vssp.nummaps", 4));
		
		try {
			if (iteration > 0) {
				FileInputFormat.addInputPath(job, new Path(itergoutprev));
			} else {
				FileInputFormat.addInputPath(job, new Path(inPath));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
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
		
		return job;
	}

	// customize the configuration going into the hadoop job
	private static Configuration applyCustomConfSettings(Configuration conf, int iteration) {
		conf.setBoolean("mapreduce.map.speculative", false);
		conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
				// org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,nl.basjes.hadoop.io.compress.SplittableGzipCodec,org.apache.hadoop.io.compress.BZip2Codec

		// buffer for mark-reset functionality, anything larger than this is cached to disk
		// configuration handled in class org.apache.hadoop.mapred.BackupStore
//		conf.set("mapreduce.reduce.markreset.buffer.size", 10000000 + "");
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
//		if (flagDoCompress) {
//			conf.set("mapreduce.input.fileinputformat.split.minsize", "3000000");
//			conf.set("mapreduce.input.fileinputformat.split.maxsize", "3000000");
//		} else {
//			conf.set("mapreduce.input.fileinputformat.split.minsize", "60000000");
//			conf.set("mapreduce.input.fileinputformat.split.maxsize", "60000000");
//		}

		// dbpedia
		//		conf.set("mapreduce.input.fileinputformat.split.minsize", "900000000");
		//		conf.set("mapreduce.input.fileinputformat.split.maxsize", "1000000000");
		conf.set("mapreduce.task.io.sort.mb", "100");
		
		if (flagDoCompress) {
			conf.setBoolean("mapred.compress.map.output", true);
			conf.setBoolean("mapreduce.map.output.compress", true);

			// potential gz version
//			conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
//			conf.set("mapred.output.compression.type", "BLOCK");
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
