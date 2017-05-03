package project.Transportation.Delay;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SecondarySort.IntPair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Partitioner;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.opencsv.CSVReader;

public class BusiestAirport {

	public static int YEAR = 4, QUARTER = 5, ORIGIN = 6, PASSENGERS = 27, DEST = 14, DISTANCE = 29;

	public static class AirportMapper extends Mapper<LongWritable, Text, IntPair, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] nextLine = value.toString().split(",");

			IntPair mapKey = new IntPair();
			Text mapValue = new Text();

			if (nextLine[0].replace("\"", "").equals("ItinID") || nextLine[YEAR].isEmpty() || nextLine[QUARTER].isEmpty()
					|| nextLine[ORIGIN].isEmpty() || nextLine[PASSENGERS].isEmpty() || nextLine[DEST].isEmpty()
					|| nextLine[DISTANCE].isEmpty()) {
				return;
			} else {
				int parsedYear = Integer.parseInt(nextLine[YEAR]);
				int parsedQuarter = Integer.parseInt(nextLine[QUARTER]);
				// int parsedPassengers =
				// Integer.parseInt(nextLine[PASSENGERS]);

				mapKey.set(parsedYear, parsedQuarter);
				mapValue.set(new Text(nextLine[DEST].replace("\"", "") + "@" + nextLine[PASSENGERS]));

				context.write(mapKey, mapValue);
			}
		}
	}

	public static class AirportPartitioner extends Partitioner<IntPair, Text> {

		@Override
		public int getPartition(IntPair key, Text value, int numPartitions) {

			// partition the flights to different reducers
			return (key.getFirst() * 127) % numPartitions;
		}

	}

	public static class KeyComparator extends WritableComparator {

		protected KeyComparator() {
			super(IntPair.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {

			IntPair ip1 = (IntPair) w1;
			IntPair ip2 = (IntPair) w2;

			int cmp = Integer.compare(ip1.getFirst(), ip2.getFirst());

			if (cmp != 0)
				return cmp;
			return Integer.compare(ip1.getSecond(), ip2.getSecond());
		}
	}

	public static class GroupComparator extends WritableComparator {

		protected GroupComparator() {
			super(IntPair.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			IntPair ip1 = (IntPair) w1;
			IntPair ip2 = (IntPair) w2;

			return Integer.compare(ip1.getFirst(), ip2.getFirst());

		}
	}

	public static class AirportReducer extends Reducer<IntPair, Text, NullWritable, Text> {

		private NullWritable nullkey = NullWritable.get();

		public void reduce(IntPair key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Double> map = new HashMap<String, Double>();
			int prevQuarter = 0;
			String result = key.getFirst() + "";
			for (Text v : values) {
				String record = v.toString();
				String[] splitRecord = record.split("@");
				String mapKey = splitRecord[0];
				if (prevQuarter == key.getSecond()) {
					if (map.containsKey(mapKey)) {
						double count = map.get(mapKey);
						count = count + Double.parseDouble(splitRecord[1]);
						map.put(mapKey, count);
					} else {
						map.put(mapKey, Double.parseDouble(splitRecord[1]));
					}
				} else {
					Double max = 0.0;
					String dest = "";
					for (Map.Entry<String, Double> entry : map.entrySet()) {
						if (entry.getValue() > max) {
							max = entry.getValue();
							dest = entry.getKey();

						}
					}
					if (dest != "")
						result = result + ",Q" + prevQuarter + "-" + dest;
					map.clear();
					map.put(mapKey, Double.parseDouble(splitRecord[1]));
					prevQuarter = key.getSecond();

				}

			}
			Double max = 0.0;
			String dest = "";
			for (Map.Entry<String, Double> entry : map.entrySet()) {
				if (entry.getValue() > max) {
					max = entry.getValue();
					dest = entry.getKey();

				}
			}
			result = result + ",Q" + key.getSecond() + "-" + dest;

			context.write(nullkey, new Text(result));
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
		job.setJarByClass(BusiestAirport.class);
		job.setMapperClass(AirportMapper.class);
		job.setReducerClass(AirportReducer.class);
		job.setNumReduceTasks(10);
		job.setPartitionerClass(AirportPartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
