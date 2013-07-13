package org.clueweb.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableUtils;

public class VByteDocVector implements DocVector {
  private int[] termids;

  public VByteDocVector() {}

  public int[] getTermIds() {
    return termids;
  }

  public int getLength() {
    return termids.length;
  }

  public static void fromBytesWritable(BytesWritable bytes, VByteDocVector doc) {
    try {
      ByteArrayInputStream bytesIn = new ByteArrayInputStream(bytes.getBytes());
      DataInputStream data = new DataInputStream(bytesIn);

      int length = WritableUtils.readVInt(data);
      doc.termids = new int[length];
      for (int i = 0; i < length; i++) {
        doc.termids[i] = WritableUtils.readVInt(data);
      }
    } catch (IOException e) {
      doc.termids = new int[0];
    }
  }

  public static void toBytesWritable(BytesWritable bytes, int[] termids, int length) {
    try {
      if (termids == null) {
        termids = new int[] {};
        length = 0;
      }

      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream dataOut = new DataOutputStream(bytesOut);

      WritableUtils.writeVInt(dataOut, length);
      for (int i = 0; i < length; i++) {
        WritableUtils.writeVInt(dataOut, termids[i]);
      }

      byte[] raw = bytesOut.toByteArray();
      bytes.set(raw, 0, raw.length);
    } catch (IOException e) {
      bytes.set(new byte[] {}, 0, 0);
    }
  }
}
