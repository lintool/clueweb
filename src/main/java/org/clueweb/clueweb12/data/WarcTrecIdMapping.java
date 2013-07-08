package org.clueweb.clueweb12.data;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

public class WarcTrecIdMapping {
  public static enum IndexField {
    WARC_TREC_ID("WARC-TREC-ID");

    public final String name;

    IndexField(String s) {
      name = s;
    }
  };

  public static void main(String[] args) throws IOException {
    IndexReader reader = DirectoryReader.open(FSDirectory.open(new File("ids")));
    IndexSearcher searcher = new IndexSearcher(reader);

    Query query = new TermQuery(
        new Term(IndexField.WARC_TREC_ID.name, "clueweb12-0000tw-00-00000"));

    TopDocs rs = searcher.search(query, 1);
    System.out.println(rs.scoreDocs[0].doc);

    System.out.println(reader.document(772745).getField(IndexField.WARC_TREC_ID.name).stringValue());
  }

}
