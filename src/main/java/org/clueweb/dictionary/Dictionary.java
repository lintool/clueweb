package org.clueweb.dictionary;

/**
 * A dictionary provides a bidirectional mapping terms (Strings) and term ids (integers). The
 * semantics of the mapping is left unspecified, but the iteration order is always in
 * increasing term id. 
 *
 * @author Jimmy Lin
 */
public interface Dictionary extends Iterable<String> {
  /**
   * Returns the term associated with this term id.
   *
   * @param id term id
   * @return term associated with this term id
   */
  String getTerm(int id);

  /**
   * Returns the id associated with this term.
   *
   * @param term term
   * @return id associated with this term
   */
  int getId(String term);

  /**
   * Returns the size of this dictionary.
   *
   * @return number of terms in this dictionary
   */
  int size();
}
