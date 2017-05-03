package project.Transportation.Delay;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.examples.SecondarySort.IntPair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TransportCompute extends Configured implements Tool {

    static Configuration conf;
    static FileSystem fs = null;

    static ArrayList<String> busiestAirports = new ArrayList<String>();
    //static

    public static class TransportMapper extends TableMapper<IntPair, Text> {
        //String[] busiestRecord;
        
        @Override
        protected void setup(Mapper<ImmutableBytesWritable, Result, IntPair, Text>.Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            String Params = conf.get("PartFiles");
            //int counter = 0;
            Path pt = null;
            Path newPattern = null;
            try {
                pt = new Path(new URI(Params));
                newPattern = new Path(pt, "part-r-*");

            } catch (URISyntaxException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }// Location of file in HDFS
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] list = fs.globStatus(newPattern);
            for (FileStatus status : list) {
                BufferedReader br = new BufferedReader(new InputStreamReader(
                        fs.open(status.getPath())));
                String line;
                // line=
                while ((line = br.readLine()) != null) {

                    // System.out.println("line count: "
                    // +counter+busiestAirports[counter]);
                    //System.out.println("line " + counter + ": " + line);
                    busiestAirports.add(line);
                    //busiestAirports[counter] = line;
                    // line=br.readLine();
                    //counter++;

                }

            }
            //System.out.println("BusiestAirport length " + busiestAirports[0]
            //        + busiestAirports[1]);
        }

        public void map(ImmutableBytesWritable key, Result value,
                Context context) throws IOException, InterruptedException {

            IntPair MapKey = new IntPair();
            Text MapValue = new Text();
            //System.out.println("linearray"+busiestAirports.size());
            String delayMin = new String(value.getValue(
                    Bytes.toBytes("transportData"), Bytes.toBytes("delayMin")));

            String originCity = new String(
                    value.getValue(Bytes.toBytes("transportData"),
                            Bytes.toBytes("originCity")));

            String destinationCity = new String(value.getValue(
                    Bytes.toBytes("transportData"),
                    Bytes.toBytes("destinationCity")));

            String[] partsOfK = Bytes.toString(key.get()).split("%");

            String year = partsOfK[0];
            String quarter = partsOfK[1];

            if (quarter.isEmpty() || year.isEmpty() || delayMin.isEmpty()
                    || originCity.isEmpty() || destinationCity.isEmpty()) {
                return;
            }

            if (year.equals("Year")) {
                return;
            }

            // System.out.println(airLineID);

            int parsedYear = Integer.parseInt(String.valueOf(year));
            
            int parsedQuarter = Integer.parseInt(String.valueOf(quarter));
            for (int i = 0; i < busiestAirports.size(); i++) {
                String[] busiestRecord = busiestAirports.get(i).split(",");
                int busiestRecordsize = busiestRecord.length;
                if (parsedYear == Integer.parseInt(busiestRecord[0])) {
                	//System.out.println("Year");
                    for (int y = 1; y < busiestRecordsize; y++) {
                    	
                        if (parsedQuarter == Integer.parseInt(String.valueOf(busiestRecord[y].charAt(1))) && destinationCity.equals(busiestRecord[y].substring(3))) {
                            MapKey.set(parsedYear, parsedQuarter);
                            MapValue.set(delayMin + "%" + destinationCity);
                            context.write(MapKey, MapValue);

                        }
                    }

                }
            }

        }
    }

    public static class TransportReducer extends
            Reducer<IntPair, Text, Text, Text> {

        public void reduce(IntPair key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            float countSum = 0;
            int countFlight = 0;

            String result = "";
            int prevquarter = 0;

            Text ReducerKey = new Text();
            Text ReducerValue = new Text();

            String result1 = key.getFirst() + ",";

            double delayCal;
            double delay;

            String[] record = { "" };

            for (Text t : values) {
                String value = t.toString();
                record = value.split("%");

                // System.out.println("inside for loop");

                if (key.getSecond() == prevquarter) {
                    countSum = countSum + Float.parseFloat(record[0]);
                    countFlight = countFlight + 1;

                } else {
                    if (countSum != 0) {

                        delayCal = countSum / countFlight;
                        delay = Math.ceil(delayCal);
                        result += "(" + prevquarter + " " + record[1] + "-"
                                + delay + ")" + ",";

                        //System.out.println(result);

                    }
                    prevquarter = key.getSecond();
                    countFlight = 1;
                    countSum = Float.parseFloat(record[0]);

                }
            }
            delayCal = countSum / countFlight;
            delay = Math.ceil(delayCal);
            result += "(" + prevquarter + " " + record[1] + "-" + delay + ")"
                    + ",";

            ReducerKey.set(result1);
            ReducerValue.set(result);

            context.write(ReducerKey, ReducerValue);

        }
    }

    public static class TransportPartitioner extends Partitioner<IntPair, Text> {

        public int getPartition(IntPair key, Text value, int numPartitions) {

            return Math.abs(key.getFirst() * 127) % numPartitions;
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

    public static void main(String[] args) throws Exception {

        conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum", args[1]);
        conf.set("PartFiles", args[2]);

        int exitCode = ToolRunner.run(conf, new TransportCompute(), args);
        System.exit(exitCode);
    }

    public int run(String[] arg0) throws IOException {

        Job job = Job.getInstance(conf, "TransportCompute");

        Scan scan = new Scan();
        scan.setCaching(100000);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob("transportTable", scan,
                TransportMapper.class, IntPair.class, Text.class, job);

        job.setJarByClass(TransportCompute.class);
        job.setMapperClass(TransportMapper.class);
        job.setPartitionerClass(TransportPartitioner.class);
        job.setGroupingComparatorClass(GroupComparator.class);
        
        job.setReducerClass(TransportReducer.class);
        job.setNumReduceTasks(10);

        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(arg0[0]));

        TextInputFormat.setInputPaths(job, new Path(arg0[2]));

        boolean b;
        try {
            b = job.waitForCompletion(true);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return 0;

    }
}
