package com.example.testhbase.demo.mapred;

import com.example.testhbase.demo.utils.TestStringOutput;
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
    TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {


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
    ImmutableBytesWritable valueout = new ImmutableBytesWritable(
        new ObjectMapper().writeValueAsBytes(columnQualifiers));

    context.write(rowkey, valueout);
  }

}
