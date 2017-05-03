package project.Transportation.bestAirline;

import java.io.IOException;
import java.io.StringReader;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.collections.iterators.EntrySetMapIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SecondarySort.IntPair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.opencsv.CSVReader;

public class LowestDelayPercentage {

	public static class DelayMapper extends Mapper<LongWritable, Text, Text, Text> {

		static int DELAY = 39, CARRIER = 6;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] nextLine = value.toString().split(",");
			if (nextLine[0].replace("\"", "").equals("Year"))
				return;

			if (!nextLine[DELAY].isEmpty() && nextLine[DELAY] != null && !nextLine[DELAY].equals("0.00")) {
				context.write(new Text(nextLine[CARRIER].replace("\"", "")), new Text(String.valueOf(1)));
			}
			context.write(new Text(nextLine[CARRIER].replace("\"", "")), new Text("Dummy"));

		}
	}

	public static class DelayReducer extends Reducer<Text, Text, Text, Text> {

		Map<String, Double> map = new HashMap<String, Double>();

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			ValueComparator bvc = new ValueComparator(map);
			TreeMap<String, Double> sorted_map = new TreeMap<String, Double>(bvc);

			sorted_map.putAll(map);

			int count = 1;
			for (Map.Entry<String, Double> e : sorted_map.entrySet()) {
				System.out.println(e.getKey());
				context.write(new Text(e.getKey()), new Text(String.valueOf(count) +'@' + e.getValue().toString()));
				count++;
				if (count > 10)
					break;
			}

		}

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int totalFlights = 0;
			int delayedFlights = 0;

			for (Text v : values) {
				if (v.toString().equals("Dummy")) {
					totalFlights++;
				} else {
					delayedFlights++;
				}
			}
			// System.out.println(totalFlights+" "+delayedFlights);
			double delayPercentage = (delayedFlights * 100.0) / totalFlights;
			String str = String.format("%1.2f", delayPercentage);

			map.put(key.toString(), Double.valueOf(str));

			// context.write(key, new DoubleWritable(delayPercentage));

		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: flights <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Delay");
		job.setJarByClass(LowestDelayPercentage.class);
		job.setMapperClass(DelayMapper.class);
		job.setReducerClass(DelayReducer.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
