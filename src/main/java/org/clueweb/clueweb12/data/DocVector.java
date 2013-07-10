package org.clueweb.clueweb12.data;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableUtils;

public class DocVector {
  private int[] termids;

  public DocVector() {
  }

  public int[] getTermIds() {
    return termids;
  }

  public int getLength() {
    return termids.length;
  }

  public static DocVector fromBytesWritable(BytesWritable bytes, DocVector doc) {
    try {
      ByteArrayInputStream bytesIn = new ByteArrayInputStream(bytes.getBytes());
      DataInputStream data = new DataInputStream(bytesIn);

      int length = WritableUtils.readVInt(data);
      doc.termids = new int[length];
      for (int i = 0; i < length; i++) {
        doc.termids[i] = WritableUtils.readVInt(data);
      }
      return doc;
    } catch (IOException e) {
      return doc;
    }
  }
}
