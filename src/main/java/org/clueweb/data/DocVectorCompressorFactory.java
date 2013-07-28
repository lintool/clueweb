package org.clueweb.data;

public class DocVectorCompressorFactory {
  public static DocVectorCompressor getCompressor(String type) {
    if (type.equalsIgnoreCase("pfor")) {
      return new PForDocVector.Compressor();
    } else if (type.equals("vbyte")) {
      return new VByteDocVector.Compressor();
    }

    return null;
  }

  public static String getOptions() {
    return "pfor|vbyte";
  }
}
