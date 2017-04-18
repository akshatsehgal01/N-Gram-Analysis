/*
 * Class Name - WordCountByLength.java
 * 
 * Authors:
 * Ms. Saatchi Nandwani
 * Mr. Akshat Sehgal
 * 
 * Description:
 * WordCountByLength program calculates the number
 * of n-letter words in any text file, calculates
 * the total time taken by the map reduce job and
 * prints it to console.
 */

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountByLength {

	public static class WordCountByLengthMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		// creating key instance
		private Text wordLength = new Text();

		// creating value instance
		private final static IntWritable one = new IntWritable(1);

		// mapper function
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String tempStr = "-letter words";

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
			 * three separate words.
			 * Regex for above logic - "\\W+"
			 */
			String[] wordArray = textFileString.split("\\W+");

			/*
			 * iterate through word array and use length of each word as key
			 */
			for (int i = 0; i < wordArray.length; i++) {
				if(wordArray[i].length()!=0){
				wordLength.set(Integer.toString(wordArray[i].length())
						+ tempStr);
				context.write(wordLength, one);
				}
			}
		}
	}

	public static class WordCountByLengthReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		// reducer function
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			/*
			 * n stores the number of occurrences of each n-letter word
			 */
			int number = 0;

			/*
			 * iterate through all the values to find number of values
			 * corresponding to a particular key(n-letter word)
			 */
			for (IntWritable v : values) {
				number += v.get();
			}
			result.set(number);
			context.write(key, result);
		}
	}

	// driver function
	public static void main(String[] args) throws Exception {

		// calculate the start time of our program
		double startTime = System.currentTimeMillis();

		Configuration c = new Configuration();
		String[] otherArgs = new GenericOptionsParser(c, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.exit(2);
		}
		Job job = Job.getInstance(c, "word count by length");
		job.setJarByClass(WordCountByLength.class);
		job.setMapperClass(WordCountByLengthMapper.class);
		job.setCombinerClass(WordCountByLengthReducer.class);
		job.setReducerClass(WordCountByLengthReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));

		// calculate the end-time of our program
		double endTime = System.currentTimeMillis();

		// calculate the time taken by our program to complete map-reduce job
		double totalTime = endTime - startTime;

		System.out.println("Time Taken by map reduce program:" + totalTime
				/ 1000 + "seconds");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}