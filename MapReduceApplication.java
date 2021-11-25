import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class solution2 extends Configured implements Tool{
	public static class s2map extends Mapper<Object, Text, Text, DoubleWritable> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String [] field = value.toString().split(" ");
			String carRego = field[0];
			Double speed = Double.parseDouble(field[2]);
			context.write(new Text(carRego), new DoubleWritable(speed));
		}
	}

	public static class s2reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> value, Context context) throws IOException, InterruptedException {
			DoubleWritable avgSpeed = new DoubleWritable();
			int totalSpeed = 0;
			int count = 0;
			int limit = 60;
	
			for (DoubleWritable val : value) {
				if (val.get() > limit) {
					totalSpeed += val.get();
					count++;
				}
			}

			avgSpeed.set(totalSpeed / count);
			context.write(key, avgSpeed);
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "s2avg");

		job.setJarByClass(solution2.class);

		job.setMapperClass(s2map.class);
		
		job.setReducerClass(s2reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		int exit = ToolRunner.run(new solution2(), args);
		System.exit(exit);
	}
}
