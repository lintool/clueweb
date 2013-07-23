package org.clueweb.data;

import static org.junit.Assert.assertEquals;

import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

public class VByteDocVectorTest {
	private static final Random RANDOM = new Random();

	@Test
	public void testSerialize1() throws Exception {
		int[] doc = new int[256];
		for (int i = 0; i < 256; i++) {
			doc[i] = RANDOM.nextInt(10000);
		}

		BytesWritable bytes = new BytesWritable();
		VByteDocVector.toBytesWritable(bytes, doc, 256);

		VByteDocVector v = new VByteDocVector();
		VByteDocVector.fromBytesWritable(bytes, v);

		assertEquals(doc.length, v.getLength());
		for (int i = 0; i < doc.length; i++) {
			assertEquals(doc[i], v.getTermIds()[i]);
		}
	}

	// Make sure serializing an empty document works.
	@Test
	public void testSerialize2() throws Exception {
		BytesWritable bytes = new BytesWritable();
		VByteDocVector.toBytesWritable(bytes, new int[] {}, 0);

		VByteDocVector v = new VByteDocVector();
		VByteDocVector.fromBytesWritable(bytes, v);

		assertEquals(0, v.getLength());
		assertEquals(0, v.getTermIds().length);
	}

	// Make sure serializing a "null" document works.
	@Test
	public void testSerialize3() throws Exception {
		BytesWritable bytes = new BytesWritable();
		VByteDocVector.toBytesWritable(bytes, new int[] {}, 0);

		VByteDocVector v = new VByteDocVector();
		VByteDocVector.fromBytesWritable(bytes, v);

		assertEquals(0, v.getLength());
		assertEquals(0, v.getTermIds().length);
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(VByteDocVectorTest.class);
	}
}
