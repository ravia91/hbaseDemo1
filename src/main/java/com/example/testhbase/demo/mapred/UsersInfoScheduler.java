package com.example.testhbase.demo.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class UsersInfoScheduler {

  @GetMapping("countTime")
  public ResponseEntity extractCounts() {
    mainTest();
    return ResponseEntity.ok().build();
  }

  public static void main(String args[]) {
    mainTest();
  }

  static void mainTest() {
    try {

      Configuration conf = HBaseConfiguration.create();
      conf.set("hbase.zookeeper.quorum",
          "127.0.0.1");
      conf.set("hbase.zookeeper.property.clientPort",
          "2181");

//      conf.set("hbase.mapreduce.inputtable", "product");
//      conf.set("hbase.mapreduce.scan", convertScanToString(scan));

      Job job = Job.getInstance(conf, "pageViewCounts");
      Scan scan = new Scan();
      scan.setStartRow(Bytes.toBytes("ST_000503879_28"));
      scan.setFilter(new PrefixFilter(Bytes.toBytes("ST_000503")));

//      scan.setCaching(1000);

      // Create a scan

      // Configure the Map process to use HBase
      TableMapReduceUtil.initTableMapperJob(

          "product",                    // The name of the table
          scan,                           // The scan to execute against the table
          UsersTimeMapper.class,                 // The Mapper class
          ImmutableBytesWritable.class,             // The Mapper output key class
          LongWritable.class,             // The Mapper output value class
          job);  // The Hadoop job

      // Configure the reducer process
      job.setReducerClass(UsersTimeReducer.class);
//      job.setCombinerClass(UsersTimeReducer.class);

      // Setup the output - we'll write to the file system: HOUR_OF_DAY   PAGE_VIEW_COUNT
      job.setOutputKeyClass(LongWritable.class);
      job.setOutputValueClass(LongWritable.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      // We'll run just one reduce task, but we could run multiple
      job.setNumReduceTasks(1);

      // Write the results to a file in the output directory
      FileOutputFormat.setOutputPath(job, new Path("output"));

      // Execute the job
      job.waitForCompletion(true);


    } catch (Exception e) {

    }
  }


}
