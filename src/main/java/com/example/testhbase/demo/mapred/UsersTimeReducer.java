package com.example.testhbase.demo.mapred;

import java.io.IOException;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

@NoArgsConstructor
public class UsersTimeReducer extends
    Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

  @Override
  protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
    // Add up all of the page views for this hour
    long sum = 0;
    for (LongWritable count : values) {
      sum += count.get();
    }

    // Write out the current hour and the sum
    context.write(key, new LongWritable(sum));
  }
}
