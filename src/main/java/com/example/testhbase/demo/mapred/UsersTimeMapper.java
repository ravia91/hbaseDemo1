package com.example.testhbase.demo.mapred;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NavigableMap;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;

@NoArgsConstructor
@Slf4j
public class UsersTimeMapper extends
    TableMapper<ImmutableBytesWritable, LongWritable> {

  private LongWritable ONE = new LongWritable(1);

  @Override
  protected void map(ImmutableBytesWritable rowkey, Result columns, Context context)
      throws IOException, InterruptedException {

    NavigableMap<byte[], byte[]> styleColumns = columns.getFamilyMap(Bytes.toBytes("style"));
    Iterator<byte[]> styleColumnNamesIterator = styleColumns.navigableKeySet().iterator();

    HashMap<String, String> columnQualifiers = new HashMap<>();

    while (styleColumnNamesIterator.hasNext()) {
      byte[] colKey = styleColumnNamesIterator.next();
      String key = Bytes.toString(colKey);
      String value = Bytes.toString(styleColumns.get(colKey));
      columnQualifiers.put(key, value);
    }
    log.info(
        "received style attributes: " + new ObjectMapper().writeValueAsString(columnQualifiers));
    context.write(new ImmutableBytesWritable("rowCount".getBytes()), ONE);
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
