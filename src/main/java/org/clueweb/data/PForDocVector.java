package org.clueweb.data;

import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.VariableByte;
import tl.lin.data.array.IntArrayWritable;

public class PForDocVector {
  private static final FastPFOR P4 = new FastPFOR();
  private static final VariableByte VB = new VariableByte();

  private int[] termids;

  public PForDocVector() {}

  public int[] getTermIds() {
    return termids;
  }

  public int getLength() {
    return termids.length;
  }

  public static void fromIntArrayWritable(IntArrayWritable in, PForDocVector doc) {
    try {
      int[] compressed = in.getArray();
      IntWrapper inPos = new IntWrapper(1);
      IntWrapper outPos = new IntWrapper(0);
      doc.termids = new int[compressed[0]];

      if (doc.termids.length == 0) {
        return;
      }

      if (doc.termids.length < 128) {
        VB.uncompress(compressed, inPos, in.size()-1, doc.termids, outPos);
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
      doc.termids = new int[0];
    }
  }

  public static void toIntArrayWritable(IntArrayWritable ints, int[] termids, int length) {
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

      P4.compress(termids, inPos, (length/128)*128, out, outPos);

      if (length % 128 == 0) {
        ints.setArray(out, outPos.get());
        return;
      }

      VB.compress(termids, inPos, length % 128, out, outPos);
      ints.setArray(out, outPos.get());
    } catch (Exception e) {
      e.printStackTrace();
      ints.setArray(new int[] {}, 0);
    }
  }
}
