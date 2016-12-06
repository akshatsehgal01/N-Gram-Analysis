/*
 * Class Name - TrigramCountST.java
 * 
 * Authors:
 * Ms. Saatchi Nandwani
 * Mr. Akshat Sehgal
 * 
 * Description:
 * TrigramCountST program calculates the number
 * of trigrams in any text file, calculates
 * the total time taken by the map reduce job and
 * prints it to console.
 */

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TrigramCountST {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		private static final IntWritable ONE = new IntWritable(1);
		private static final Text TRIGRAM = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String prev = null;
			String cur = null;
			String cur1 = null;
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				if (prev == null) {
					cur = itr.nextToken();
				}
				if (itr.hasMoreTokens()) {
					cur1 = itr.nextToken();
				}
				// Emit only if we have an actual bigram.
				if (prev != null) {
					TRIGRAM.set(prev + " " + cur + " " + cur1);
					context.write(TRIGRAM, ONE);
				}
				prev = cur;
				cur = cur1;
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable SUM = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			Iterator<IntWritable> iter = values.iterator();
			while (iter.hasNext()) {
				sum += iter.next().get();
			}
			SUM.set(sum);
			context.write(key, SUM);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String inputPath = args[0];
		String outputPath = args[1];
		int reduceTasks = Integer.parseInt(args[2]);

		if (args.length < 3) {
			System.err
					.println("TrigramCount usage: [input-path] [output-path] [num-reducers]");
			System.exit(0);
		}

		Job job = Job.getInstance(conf, "trigram count");
		job.setJobName(TrigramCountST.class.getSimpleName());
		job.setJarByClass(TrigramCountST.class);

		job.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}