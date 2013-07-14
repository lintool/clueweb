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
  public void testCode() throws Exception {
    FastPFOR p4 = new FastPFOR();
    int[] doc = new int[256];
    for (int i = 0; i<256; i++) {
      doc[i] = RANDOM.nextInt(10000);
    }

    IntWrapper inPos = new IntWrapper(0);
    IntWrapper outPos = new IntWrapper(0);

    //System.out.println(doc.length + ": " + Arrays.toString(doc));
    int[] out = new int[256];
    p4.compress(doc, inPos, doc.length, out, outPos);

    int[] trimmedOut = new int[outPos.get()];
    System.arraycopy(out, 0, trimmedOut, 0, outPos.get());

    assertTrue(trimmedOut.length < doc.length);
    //System.out.println(trimmedOut.length + ": " + Arrays.toString(trimmedOut));

    IntWrapper cinPos = new IntWrapper(0);
    IntWrapper coutPos = new IntWrapper(0);
    int[] reconstructed = new int[256];
    p4.uncompress(trimmedOut, cinPos, trimmedOut.length, reconstructed, coutPos);

    //System.out.println(cinPos + " " + coutPos);
    //System.out.println(reconstructed.length + ": " + Arrays.toString(reconstructed));
    
    assertEquals(doc.length, reconstructed.length);
    for (int i = 0; i < doc.length; i++) {
      assertEquals(doc[i], reconstructed[i]);
    }
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