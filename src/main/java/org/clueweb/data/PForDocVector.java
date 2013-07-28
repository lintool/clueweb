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

import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.VariableByte;

import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import tl.lin.data.array.IntArrayWritable;

public class PForDocVector implements DocVector {
  private static final Logger LOG = Logger.getLogger(PForDocVector.class);

  private int[] termids;

  public PForDocVector() {}

  public int[] getTermIds() {
    return termids;
  }

  public int getLength() {
    return termids.length;
  }

  public static class Compressor implements DocVectorCompressor {
    private static final FastPFOR P4 = new FastPFOR();
    private static final VariableByte VB = new VariableByte();
    private static final IntArrayWritable EMPTY = new IntArrayWritable(new int[] {0});

    @Override
    public void decompress(Writable writable, DocVector docvector) {
      IntArrayWritable in = (IntArrayWritable) writable;
      PForDocVector doc = (PForDocVector) docvector;
      try {
        int[] compressed = in.getArray();
        IntWrapper inPos = new IntWrapper(1);
        IntWrapper outPos = new IntWrapper(0);
        doc.termids = new int[compressed[0]];

        if (doc.termids.length == 0) {
          return;
        }

        if (doc.termids.length < 128) {
          VB.uncompress(compressed, inPos, in.size() - 1, doc.termids, outPos);
          return;
        }

        // For this, the zero doesn't matter.
        P4.uncompress(compressed, inPos, 0, doc.termids, outPos);

        if (doc.termids.length % 128 == 0) {
          return;
        }

        // Decode whatever is left over.
        VB.uncompress(compressed, inPos, in.size() - inPos.get(), doc.termids, outPos);
      } catch (Exception e) {
        e.printStackTrace();
        LOG.error("Error decompressing document vector!");
        doc.termids = new int[0];
      }
    }

    @Override
    public void compress(Writable writable, int[] termids, int length) {
      IntArrayWritable ints = (IntArrayWritable) writable;
      // Remember, the number of terms to serialize is length; the array might be longer.
      try {
        if (termids == null) {
          termids = new int[] {};
          length = 0;
        }

        IntWrapper inPos = new IntWrapper(0);
        IntWrapper outPos = new IntWrapper(1);

        int[] out = new int[length + 1];
        out[0] = length;

        if (length < 128) {
          VB.compress(termids, inPos, length, out, outPos);
          ints.setArray(out, outPos.get());

          return;
        }

        P4.compress(termids, inPos, (length / 128) * 128, out, outPos);

        if (length % 128 == 0) {
          ints.setArray(out, outPos.get());
          return;
        }

        VB.compress(termids, inPos, length % 128, out, outPos);
        ints.setArray(out, outPos.get());
      } catch (Exception e) {
        e.printStackTrace();
        LOG.error("Error compressing document vector!");
        ints.setArray(new int[] {}, 0);
      }
    }

    @Override
    public Writable emptyDocVector() {
      return EMPTY;
    }

    @Override
    public Class<? extends Writable> getCompressedClass() {
      return IntArrayWritable.class;
    }

    @Override
    public DocVector createDocVector() {
      return new PForDocVector();
    }
  }
}
