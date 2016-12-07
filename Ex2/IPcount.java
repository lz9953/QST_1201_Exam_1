package Test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Ipcount {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
	
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String ip = value.toString().split("\t")[0];
			context.write(new Text(ip), new Text());
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, IntWritable>{
		int num =0;
		@Override
		protected void reduce(Text key, Iterable<Text> value,Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			num++;
		}
		@Override
		protected void cleanup(Context context)throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(new Text("count = "), new IntWritable(num));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "wordcount");
		job.setJarByClass(Ipcount.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));;
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0:1);
		
	}
}

