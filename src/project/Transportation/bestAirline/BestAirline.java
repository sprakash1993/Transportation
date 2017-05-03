package project.Transportation.bestAirline;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

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

public class BestAirline {
	
public static class AirlineMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] nextLine=value.toString().split("\t");
			String[] val = nextLine[1].split("@");
			context.write(new Text(nextLine[0]), new Text(val[0]));
			
		}
	}
	
	public static class AirlineReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		
		Map<String, Double> map = new HashMap<String, Double>();

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			
			ValueComparator bvc =  new ValueComparator(map);
		    TreeMap<String,Double> sorted_map = new TreeMap<String,Double>(bvc);
			
		    sorted_map.putAll(map);
		    
		    int count = 1;
		    for(Map.Entry<String, Double> e : sorted_map.entrySet()) {
		    	System.out.println(e.getKey());
		    	context.write(new Text(e.getKey()), new DoubleWritable(e.getValue()));
		    	count++;
		    	if(count>10) break;
		    }
			
		}


		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			int rankSum=0;
			
			int count=0;
			for(Text v :values) {
				count+=1;
				rankSum+=Integer.parseInt(v.toString());
				
				
			}
			while(count!=4) {
				count+=1;
				rankSum+=11;
			}
			
			//System.out.println(totalFlights+" "+delayedFlights);
			double rankDouble = (rankSum*1.0) /4;
			String str = String.format("%1.2f", rankDouble);
			
			map.put(key.toString(), Double.valueOf(str));
			
			//context.write(key, new DoubleWritable(delayPercentage));
			
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 5) {
			System.err.println("Usage: flights <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Delay");
		job.setJarByClass(LowestDelayPercentage.class);
		job.setMapperClass(AirlineMapper.class);
		job.setReducerClass(AirlineReducer.class);
		job.setNumReduceTasks(5);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[3]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[4]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}


}
