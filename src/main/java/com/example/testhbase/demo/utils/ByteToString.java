package com.example.testhbase.demo.utils;

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

@Slf4j
public class ByteToString {

  public static void main(String[] args) throws IOException {

//    byte[] bytes = new byte[] {72,6f,77,43,6f,75,6e,74,1};
//    // System.out.println(Arrays.toString(bytes));
//
//    // Create a string from the byte array without specifying
//    // character encoding
//    String string = new String(bytes);
//    System.out.println(string);

    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum",
        "sfdllmn002.gid.gap.com");
    conf.set("hbase.zookeeper.property.clientPort",
        "2181");

    FileSystem fs = FileSystem.get(conf);
    FileStatus[] status = fs.listStatus(new Path("file:///Users/annadirv/GAP/hbasedemo/output"));
    for (int i = 0; i < status.length; i++) {
      FSDataInputStream fsDataInputStream = fs.open(status[i].getPath());
//      log.info("read line: " + );
    }
  }

}
