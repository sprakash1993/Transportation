package project.Transportation.Delay;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
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

public class TransportPopulate extends Configured implements Tool {

	static Configuration conf;

	static int year = 0;
	static int quarter = 1;
	static int origin = 11;
	static int destination = 17;
	static int delay = 37;
	static int count = 1;

	public static class TransportMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

		ImmutableBytesWritable MapKey = new ImmutableBytesWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			CSVReader reader = new CSVReader(new StringReader(value.toString()));
			String[] transportRecord = reader.readNext();

			Put p = new Put(Bytes.toBytes(transportRecord[year] + "%" + transportRecord[quarter] + "%" + count));

			p.add(Bytes.toBytes("transportData"), Bytes.toBytes("originCity"), Bytes.toBytes(transportRecord[origin]));
			p.add(Bytes.toBytes("transportData"), Bytes.toBytes("destinationCity"),
					Bytes.toBytes(transportRecord[destination]));
			p.add(Bytes.toBytes("transportData"), Bytes.toBytes("delayMin"), Bytes.toBytes(transportRecord[delay]));

			MapKey.set(p.getRow());
			context.write(MapKey, p);
			count++;

			reader.close();

		}
	}

	public static void main(String[] args) throws Exception {

		conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);

        boolean tableExists = admin.tableExists("transportTable");

        if (tableExists) {
            admin.disableTable("transportTable");
            admin.deleteTable("transportTable");
        }

        String inputPath = args[0];
        String tableName = args[1];

        conf.set("hbase.mapred.outputtable", tableName);

        HTableDescriptor htd = new HTableDescriptor(tableName);
        htd.addFamily(new HColumnDescriptor("transportData"));
        admin.createTable(htd);

        conf.set("hbase.zookeeper.quorum", args[2]);

        int exitCode = ToolRunner.run(conf, new TransportPopulate(), args);

        System.exit(exitCode);
        admin.close();
	}

	public int run(String[] arg0) throws Exception {

		Job job = Job.getInstance(conf, "TransportPopulate");
        job.setJarByClass(TransportPopulate.class);
        job.setMapperClass(TransportMapper.class);
        job.setJobName("load csv data into hbase");

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        job.setNumReduceTasks(0);

        TextInputFormat.setInputPaths(job, new Path(arg0[0]));

        job.waitForCompletion(true);
        return 0;

	}
}
