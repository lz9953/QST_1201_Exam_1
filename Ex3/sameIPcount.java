package Hadoop_project;


import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.qst.common.Model.Map;
import com.qst.common.Model.Reduce;

public class CountSameIP {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			InputSplit inputsplit = context.getInputSplit();
			String filename = ((FileSplit) inputsplit).getPath().toString();
			String[] line = value.toString().split("/t");
			String ip = line[0];
			context.write(new Text(filename), new Text(ip));
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, IntWritable>{
		public void reduce(Text key, Iterator<Text> value, Context context)throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Set<String> ips = new HashSet<String>();
			while(value.hasNext()){
				ips.add(value.next().toString());
			}
			context.write(new Text(), new IntWritable(ips.size()));
		}
	}
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"temp");
		job.setJarByClass(CountSameIP.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		try {
			System.exit(job.waitForCompletion(true) ? 0:1);//提交job成功，推出JVM虚拟机。
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
