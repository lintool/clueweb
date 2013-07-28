/*
 * ClueWeb Tools: Hadoop tools for manipulating ClueWeb collections
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.clueweb.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

public class VByteDocVector implements DocVector {
  private static final Logger LOG = Logger.getLogger(VByteDocVector.class);

  private int[] termids;

  public VByteDocVector() {
  }

  public int[] getTermIds() {
    return termids;
  }

  public int getLength() {
    return termids.length;
  }

  public static class Compressor implements DocVectorCompressor {
    private static final BytesWritable EMPTY = new BytesWritable();
    static {
      EMPTY.set(new byte[] { 0 }, 0, 1);
    }

    @Override
    public void decompress(Writable writable, DocVector docvector) {
      BytesWritable bytes = (BytesWritable) writable;
      VByteDocVector doc = (VByteDocVector) docvector;
      try {
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(bytes.getBytes());
        DataInputStream data = new DataInputStream(bytesIn);

        int length = WritableUtils.readVInt(data);
        doc.termids = new int[length];
        for (int i = 0; i < length; i++) {
          doc.termids[i] = WritableUtils.readVInt(data);
        }
      } catch (IOException e) {
        e.printStackTrace();
        LOG.error("Error compressing document vector!");
        doc.termids = new int[0];
      }
    }


    @Override
    public void compress(Writable writable, int[] termids, int length) {
      BytesWritable bytes = (BytesWritable) writable;
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
        e.printStackTrace();
        LOG.error("Error decompressing document vector!");
        bytes.set(new byte[] {}, 0, 0);
      }
    }

    @Override
    public Writable emptyDocVector() {
      return EMPTY;
    }

    @Override
    public Class<? extends Writable> getCompressedClass() {
      return BytesWritable.class;
    }

    @Override
    public DocVector createDocVector() {
      return new VByteDocVector();
    }
  }
}
