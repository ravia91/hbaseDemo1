package com.example.testhbase.demo.mapred;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Calendar;
import lombok.NoArgsConstructor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

@NoArgsConstructor
public class UsersTimeMapper extends
    TableMapper<LongWritable, LongWritable> {

  private LongWritable ONE = new LongWritable( 1 );

  @Override
  protected void map(ImmutableBytesWritable rowkey, Result columns, Context context)
      throws IOException, InterruptedException {

    // Get the timestamp from the row key
    long timestamp = getTimestampFromRowKey(rowkey.get());

    // Get hour of day
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(timestamp);
    int hourOfDay = calendar.get(Calendar.HOUR_OF_DAY);

    // Output the current hour of day and a count of 1
    context.write(new LongWritable(hourOfDay), ONE);
  }

  public static long getTimestampFromRowKey(byte[] rowkey) {
    try {
      // Extract the time stamp bytes
      byte[] timestampBytes = Bytes.copy(rowkey, 16, Long.SIZE / 8);

      // Convert the byte[] to a long
      ByteArrayInputStream bais = new ByteArrayInputStream(timestampBytes);
      DataInputStream dis = new DataInputStream(bais);
      return dis.readLong();
    } catch (Exception e) {
      e.printStackTrace();
    }

    // The operation failed
    return -1;
  }

}
