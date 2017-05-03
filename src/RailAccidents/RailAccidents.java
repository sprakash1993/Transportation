
package RailAccidents;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.examples.SecondarySort.IntPair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.opencsv.CSVReader;

public class RailAccidents {

	static Configuration conf;

	// static int year = 0;
	// static int day = 1;
	// static int month;
	static int YEAR = 16, MONTH = 17, DAY = 18, TIMEHR = 19, TIMEMIN = 20, AMPM = 21, STATION = 22, COUNTY = 23,
			STATE = 24, REGION = 25, CITY = 27;
	
	
	
	public static class RailMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		HashMap<String, String> map = new HashMap<>();
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
            String Params = conf.get("codes");
            //int counter = 0;
            Path pt = null;
            Path newPattern = null;
            try {
                pt = new Path(new URI(Params));
                newPattern = new Path(pt, "statecode*");

            } catch (URISyntaxException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }// Location of file in HDFS
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] list = fs.globStatus(newPattern);
            //System.out.println("File length"+list.length);
            for (FileStatus status : list) {
                BufferedReader br = new BufferedReader(new InputStreamReader(
                        fs.open(status.getPath())));
                String line;
                // line
                while ((line = br.readLine()) != null) {
                	String[] parsedRecord = line.split(",");
                	//System.out.println(parsedRecord[0]+" "+parsedRecord[2]);
                	map.put(parsedRecord[0], parsedRecord[2]);
                	
                }
                
            }
            //System.out.println("Populated map's size " +map.size());
            //for(Entry<String, String> e : map.entrySet())
            	//System.out.println(e.getKey()+" "+e.getValue());

		}



		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			CSVReader reader = new CSVReader(new StringReader(value.toString()));
			String[] railRecord = reader.readNext();
			
			if(railRecord!=null && !railRecord[0].equals("AMTRAK")&& railRecord.length>=24)
			{
				//System.out.println("Inside mapper" +railRecord[STATE]);
				
				if(map.containsKey(railRecord[STATE]))
					context.write(new Text(map.get(railRecord[STATE])), new IntWritable(1));
			}

		}
	}
	public static class RailReducer extends Reducer<Text,IntWritable, NullWritable, Text> {
		private NullWritable nullkey = NullWritable.get();
		

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			int sum=0;
			for(IntWritable v:values) {
				sum+=v.get();
			}
			//System.out.println(key.toString()+" "+sum);
			context.write(nullkey,new Text(key.toString()+ " "+ sum));
		}
	}
public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		conf.set("codes", args[2]);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: rail <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Delay");
		job.setJarByClass(RailAccidents.class);
		job.setMapperClass(RailMapper.class);
		job.setReducerClass(RailReducer.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
