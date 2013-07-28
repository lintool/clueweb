package org.clueweb.data;

import org.apache.hadoop.io.Writable;

public interface DocVectorCompressor {
  void decompress(Writable writable, DocVector docvector);
  void compress(Writable writable, int[] termids, int length);
  Writable emptyDocVector();
  Class<? extends Writable> getCompressedClass();
  DocVector createDocVector();
}
