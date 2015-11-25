package edu.toronto.cs.sgbhadoop.hadoop220;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CounterJob {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private final Text word = new Text();

		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] parse = value.toString().split("(\t)+( )*");
			String bid = parse[1];
			if (!bid.equals("0") && bid.length() != 32) {
				bid = parse[0];
				if (!bid.equals("0") && bid.length() != 32) {
					System.err.println("ERROR IN COUNT JOB, bid not found");
				}
			}
			word.set(bid);
			context.write(word, one);
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private final IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
			//			System.out.println("counter reduce key:" + key);
			//			System.out.println("counter num:" + context.getCounter("vs", "distinct.block.count").getValue());
			// have to be careful with using counter in combiner as well (this is doubling the value)
			//			context.getCounter("vs", "distinct.block.count").increment(1);
		}
	}
}
