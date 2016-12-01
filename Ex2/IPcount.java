package Hadoop_project;


import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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

public class IPcount {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		Set<String> IPs = new HashSet<String>();//定义一个hasset列表。存储每次map处理的IP。hasset自动去重。
    @Override
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] date =  value.toString().split("\t");
			String IP = date[0];
			IPs.add(IP);
		}
    //设置一个cleanup，在所有的map运行结束后，对数据进行处理。
   public void cleanup(Context context) throws IOException, InterruptedException{
			context.write(new Text(), new IntWritable(IPs.size()));
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		@Override
		public void reduce(Text key, Iterable<Text> value, Context context)throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		
		}
	}
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"temp");
		job.setJarByClass(IPcount.class);
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
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
