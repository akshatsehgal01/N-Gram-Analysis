/*
 * Class Name - TrigramCount.java
 * 
 * Authors:
 * Ms. Saatchi Nandwani
 * Mr. Akshat Sehgal
 * 
 * Description:
 * TrigramCount program calculates the number
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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TrigramCount {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		private static final IntWritable ONE = new IntWritable(1);
		private static final Text TRIGRAM = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			/*
			 * convert input file(text type) to String type and remove beginning
			 * and ending whitespaces(if any)
			 */
			String textFileString = value.toString().trim();

			/*
			 * Split the text file string in a string array using any special
			 * character as delimiter. This is because words might be separated
			 * by a character other than space. For example - 'Tuesday,' ;
			 * 'Tuesday.' ; 'Tuesday' should be treated as Tuesday and not as
			 * three separate words. Regex for above logic - "\\W+"
			 */
			String[] wordArray = textFileString.split("\\W+");

			/*
			 * iterate through word array and find trigrams
			 */
			for (int i = 0; i < wordArray.length - 2; i++) {
				if (wordArray[i].length() != 0) {
					TRIGRAM.set(wordArray[i] + " " + wordArray[i + 1] + " "
							+ wordArray[i + 2]);
					context.write(TRIGRAM, ONE);
				}
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
		double startTime = (double) System.currentTimeMillis();
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
		job.setJobName(TrigramCount.class.getSimpleName());
		job.setJarByClass(TrigramCount.class);

		job.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		if(job.waitForCompletion(true)){
			double endTime = System.currentTimeMillis();
			System.out.println("Total time taken by the map reduce job: "+(endTime-startTime)/1000+" seconds");
			System.exit(0);
		}
		else{
			System.exit(1);
		}

	}
}