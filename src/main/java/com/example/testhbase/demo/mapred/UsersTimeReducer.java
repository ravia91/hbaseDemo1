package com.example.testhbase.demo.mapred;

import java.io.IOException;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

@NoArgsConstructor
@Slf4j
public class UsersTimeReducer extends
    Reducer<ImmutableBytesWritable, LongWritable, ImmutableBytesWritable, LongWritable> {

  @Override
  protected void reduce(ImmutableBytesWritable key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
    // Add up all of the page views for this hour
    long sum = 0;
    for (LongWritable count : values) {
      log.info("size of the records: " + count.get());
      sum += count.get();
    }

    // Write out the current hour and the sum
    log.info("writing message to output file:" + sum);

    context.write(key, new LongWritable(sum));
  }
}
