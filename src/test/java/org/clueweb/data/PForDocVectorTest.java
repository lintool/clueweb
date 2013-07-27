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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import junit.framework.JUnit4TestAdapter;
import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.IntWrapper;

import org.junit.Test;

import tl.lin.data.array.IntArrayWritable;

public class PForDocVectorTest {
  private static final Random RANDOM = new Random();

  @Test
  public void testPFor1() throws Exception {
    int len = 256;
    FastPFOR p4 = new FastPFOR();
    int[] doc = new int[len];
    for (int i = 0; i<len; i++) {
      doc[i] = RANDOM.nextInt(10000);
    }

    IntWrapper inPos = new IntWrapper(0);
    IntWrapper outPos = new IntWrapper(0);

    int[] out = new int[len];
    p4.compress(doc, inPos, doc.length, out, outPos);

    int[] trimmedOut = new int[outPos.get()];
    System.arraycopy(out, 0, trimmedOut, 0, outPos.get());

    assertTrue(trimmedOut.length < doc.length);

    IntWrapper cinPos = new IntWrapper(0);
    IntWrapper coutPos = new IntWrapper(0);
    int[] reconstructed = new int[len];
    int r = RANDOM.nextInt();
    // Interesting behavior of the PFor decompressor: r doesn't matter.
    p4.uncompress(trimmedOut, cinPos, r, reconstructed, coutPos);
    
    assertEquals(doc.length, reconstructed.length);
    for (int i = 0; i < doc.length; i++) {
      assertEquals(doc[i], reconstructed[i]);
    }
  }

  @Test
  public void testPFor2() throws Exception {
    int len = 23;
    FastPFOR p4 = new FastPFOR();
    int[] doc = new int[len];
    for (int i = 0; i<len; i++) {
      doc[i] = RANDOM.nextInt(10000);
    }

    IntWrapper inPos = new IntWrapper(0);
    IntWrapper outPos = new IntWrapper(0);

    int[] out = new int[len];
    // We're purposely only compressing 23 values here, which is smaller than a PFor block size,
    // just to see what would happen...
    p4.compress(doc, inPos, len, out, outPos);

    assertEquals(0, inPos.get());
    // Indeed, the PFor compressor refuses to process the input stream.
    assertEquals(1, outPos.get());
    // But why has the output stream incremented?
  }

  @Test
  public void testPFor3() throws Exception {
    int len = 129;
    FastPFOR p4 = new FastPFOR();
    int[] doc = new int[len];
    for (int i = 0; i<len; i++) {
      doc[i] = RANDOM.nextInt(10000);
    }

    IntWrapper inPos = new IntWrapper(0);
    IntWrapper outPos = new IntWrapper(0);

    int[] out = new int[len];
    // We're purposely only compressing 129 values here, which is one more than a PFor block.
    p4.compress(doc, inPos, len, out, outPos);

    assertEquals(128, inPos.get());
    // Indeed, the PFor compressor processes one block and that's it. (Even though we told it to
    // compress all 129 values).
  }

  @Test
  public void testPFor4() throws Exception {
    int len = 128 * 4 + 1;
    FastPFOR p4 = new FastPFOR();
    int[] doc = new int[len];
    for (int i = 0; i<len; i++) {
      doc[i] = RANDOM.nextInt(10000);
    }

    IntWrapper inPos = new IntWrapper(0);
    IntWrapper outPos = new IntWrapper(0);

    int[] out = new int[len];
    // There are multiple blocks here, but we're tell it to compress only one block.
    p4.compress(doc, inPos, 128, out, outPos);

    assertEquals(128, inPos.get());
    // And indeed, the compression complies.

    // Now tell it to compress the rest.
    p4.compress(doc, inPos, 128 * 3 + 1, out, outPos);
    assertEquals(128 * 4, inPos.get());
    // The rest of the blocks are compressed, but not the leftover integer.
  }

  @Test
  public void testSerializeRandom() throws Exception {
    testSerializeWithLength(10);
    testSerializeWithLength(25);
    testSerializeWithLength(127);
    testSerializeWithLength(128);
    testSerializeWithLength(129);
    testSerializeWithLength(255);
    testSerializeWithLength(256);
    testSerializeWithLength(257);
    testSerializeWithLength(1024);

    for (int i = 0; i < 100; i++) {
      testSerializeWithLength(RANDOM.nextInt(400));
    }
  }

  public void testSerializeWithLength(int doclength) throws Exception {
    int[] doc = new int[doclength];
    for (int i = 0; i<doclength; i++) {
      doc[i] = RANDOM.nextInt(10000);
    }

    IntArrayWritable ints = new IntArrayWritable();
    PForDocVector.toIntArrayWritable(ints, doc, doclength);

    PForDocVector v = new PForDocVector();
    PForDocVector.fromIntArrayWritable(ints, v);

    assertEquals(doclength, v.getLength());
    for (int i = 0; i < doc.length; i++) {
      assertEquals(doc[i], v.getTermIds()[i]);
    }
  }

  @Test
  public void testSerializeRandomArrayTooLong() throws Exception {
    testSerializeWithArrayTooLong(10);
    testSerializeWithArrayTooLong(25);
    testSerializeWithArrayTooLong(127);
    testSerializeWithArrayTooLong(128);
    testSerializeWithArrayTooLong(129);
    testSerializeWithArrayTooLong(255);
    testSerializeWithArrayTooLong(256);
    testSerializeWithArrayTooLong(257);
    testSerializeWithArrayTooLong(1024);

    for (int i = 0; i < 100; i++) {
      testSerializeWithArrayTooLong(RANDOM.nextInt(400));
    }
  }

  public void testSerializeWithArrayTooLong(int doclength) throws Exception {
    int[] doc = new int[doclength + RANDOM.nextInt(20)];
    for (int i = 0; i<doclength; i++) {
      doc[i] = RANDOM.nextInt(10000);
    }

    IntArrayWritable ints = new IntArrayWritable();
    PForDocVector.toIntArrayWritable(ints, doc, doclength);

    PForDocVector v = new PForDocVector();
    PForDocVector.fromIntArrayWritable(ints, v);

    assertEquals(doclength, v.getLength());
    for (int i = 0; i < doclength; i++) {
      assertEquals(doc[i], v.getTermIds()[i]);
    }
  }
  
  // Make sure serializing an empty document works.
  @Test
  public void testSerializeEmpty1() throws Exception {
    IntArrayWritable ints = new IntArrayWritable();
    PForDocVector.toIntArrayWritable(ints, new int[] {}, 0);

    PForDocVector v = new PForDocVector();
    PForDocVector.fromIntArrayWritable(ints, v);

    assertEquals(0, v.getLength());
    assertEquals(0, v.getTermIds().length);
  }

  // Make sure serializing a "null" document works.
  @Test
  public void testSerializeEmpty2() throws Exception {
    IntArrayWritable ints = new IntArrayWritable();
    PForDocVector.toIntArrayWritable(ints, null, -1);

    PForDocVector v = new PForDocVector();
    PForDocVector.fromIntArrayWritable(ints, v);

    assertEquals(0, v.getLength());
    assertEquals(0, v.getTermIds().length);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(PForDocVectorTest.class);
  }
}