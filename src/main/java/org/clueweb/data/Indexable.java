package org.clueweb.data;

import org.apache.hadoop.io.Writable;

/**
 * A document that can be indexed.
 */
public abstract class Indexable implements Writable {

  /**
   * Returns the globally-unique String identifier of the document within the collection.
   *
   * @return docid of the document
   */
  public abstract String getDocid();

  /**
   * Returns the content of the document.
   *
   * @return content of the document
   */
  public abstract String getContent();

  /**
   * Returns the content of the document for display to a human.
   *
   * @return displayable content
   */
  public String getDisplayContent() {
    return getContent();
  }

  /**
   * Returns the type of the display content, per IANA MIME Media Type (e.g., "text/html").
   * See {@code http://www.iana.org/assignments/media-types/index.html}
   *
   * @return IANA MIME Media Type
   */
  public String getDisplayContentType() {
    return "text/plain";
  }
}
