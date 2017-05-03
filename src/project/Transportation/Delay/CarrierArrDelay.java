package project.Transportation.Delay;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.opencsv.CSVReader;

public class CarrierArrDelay {

	public static class AirlineMapper extends Mapper<LongWritable, Text, Text, Text> {
		static int CARRIER = 6, DELAY=38;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			
			
			String[] nextLine = value.toString().split(",");
			
			
				
				if(nextLine[0].replace("\"", "").equals("Year")) return;
				
				if (!nextLine[DELAY].isEmpty() && nextLine[DELAY] != null) {
					
					context.write(new Text(nextLine[CARRIER].replace("\"", "")), new Text(nextLine[DELAY]));
				}
				
			
		}
	}
	
	public static class AirlineReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			double sum=0;
			
			int count=0;
			for(Text v :values) {
				count+=1;
				sum+=Double.parseDouble(v.toString());
				
				
			}
			
			
			//System.out.println(totalFlights+" "+delayedFlights);
			double avg = (sum*1.0) /count;
			String str = String.format("%1.2f", avg);
			
			
			context.write(key, new DoubleWritable(Double.parseDouble(str)));
			
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: flights <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Delay");
		job.setJarByClass(CarrierArrDelay.class);
		job.setMapperClass(AirlineMapper.class);
		job.setReducerClass(AirlineReducer.class);
		job.setNumReduceTasks(10);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
