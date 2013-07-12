package org.clueweb.clueweb12.data;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;

import tl.lin.lucene.FileSystemDirectory;

public class WarcTrecIdMapping {
  private static final Logger LOG = Logger.getLogger(WarcTrecIdMapping.class);

  public static enum IndexField {
    WARC_TREC_ID("WARC-TREC-ID");

    public final String name;

    IndexField(String s) {
      name = s;
    }
  };

  private IndexReader reader;
  private IndexSearcher searcher;

  public WarcTrecIdMapping(Path indexLocation, Configuration conf) throws IOException {
    FileSystem fs = FileSystem.getLocal(conf);
    Directory directory = new FileSystemDirectory(fs, indexLocation, false, conf);

    LOG.info("Opening index " + indexLocation);
    reader = DirectoryReader.open(directory);
    searcher = new IndexSearcher(reader);
  }

  public int getDocno(String id) {
    Query query = new TermQuery(new Term(IndexField.WARC_TREC_ID.name, id));

    TopDocs rs;
    try {
      rs = searcher.search(query, 1);
      if (rs.totalHits != 1) {
        return -1;
      }

      return rs.scoreDocs[0].doc;
    } catch (IOException e) {
      e.printStackTrace();
    }

    return -1;
  }

  public String getDocid(int docno) {
    if (docno >= reader.maxDoc()) {
      return null;
    }
    try {
      Document d = reader.document(docno);
      if (d == null) {
        return null;
      }
      return d.getField(IndexField.WARC_TREC_ID.name).stringValue();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
}
