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

import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

public class VByteDocVectorTest {
  private static final Random RANDOM = new Random();
  private static final VByteDocVector.Compressor COMPRESSOR = new VByteDocVector.Compressor(); 

  @Test
  public void testSerialize1() throws Exception {
    int[] doc = new int[256];
    for (int i = 0; i<256; i++) {
      doc[i] = RANDOM.nextInt(10000);
    }

    BytesWritable bytes = new BytesWritable();
    COMPRESSOR.compress(bytes, doc, 256);

    VByteDocVector v = new VByteDocVector();
    COMPRESSOR.decompress(bytes, v);

    assertEquals(doc.length, v.getLength());
    for (int i = 0; i < doc.length; i++) {
      assertEquals(doc[i], v.getTermIds()[i]);
    }
  }

  // Make sure serializing an empty document works.
  @Test
  public void testSerialize2() throws Exception {
    BytesWritable bytes = new BytesWritable();
    COMPRESSOR.compress(bytes, new int[] {}, 0);

    VByteDocVector v = new VByteDocVector();
    COMPRESSOR.decompress(bytes, v);

    assertEquals(0, v.getLength());
    assertEquals(0, v.getTermIds().length);
  }

  // Make sure serializing a "null" document works.
  @Test
  public void testSerialize3() throws Exception {
    BytesWritable bytes = new BytesWritable();
    COMPRESSOR.compress(bytes, new int[] {}, 0);

    VByteDocVector v = new VByteDocVector();
    COMPRESSOR.decompress(bytes, v);

    assertEquals(0, v.getLength());
    assertEquals(0, v.getTermIds().length);
  }

  // Make sure deserializing the empty document works.
  @Test
  public void testDeserializeEmpty() throws Exception {
    VByteDocVector v = new VByteDocVector();
    COMPRESSOR.decompress(COMPRESSOR.emptyDocVector(), v);

    assertEquals(0, v.getLength());
    assertEquals(0, v.getTermIds().length);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VByteDocVectorTest.class);
  }
}
