package org.clueweb.clueweb12.app;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.clueweb.data.PForDocVector;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import tl.lin.data.array.IntArrayWritable;
import tl.lin.data.pair.PairOfIntString;

public class RetrievalTest {

  private PairOfIntString keyOut;
  private FloatWritable valueOut;
  private LMRetrieval.MyMapper mapper;
  private Context mapContext;
  private IntArrayWritable iaw;
  
  @Before 
  public void setUp() {
    keyOut = new PairOfIntString();
    valueOut = new FloatWritable();
    mapper = new LMRetrieval.MyMapper();
    mapContext = mock(Context.class);  
    mapper.termidQuerySet = Maps.newHashMap();
    mapper.queryTermidSet = Maps.newHashMap();
    iaw = new IntArrayWritable();
    
    //+++++ setting up the mocked corpus +++++
    mapper.smoothingParam = 0.5;
    //stub the df values and the corpus size
    mapper.stats = mock(org.clueweb.data.TermStatistics.class);
    for(int i=0; i<ToyCorpus.df.length; i++) {
      when(mapper.stats.getDf(i)).thenReturn(ToyCorpus.df[i]);
    }
    when(mapper.stats.getCollectionSize()).thenReturn((long) ToyCorpus.numDocs);
    
    //add the query to the maps for fast access
    HashSet<Integer> query = Sets.newHashSet(); 
    HashSet<Integer> t = Sets.newHashSet(); t.add(1);
    for(int i=0; i<ToyCorpus.query.length; i++) {
      query.add(ToyCorpus.query[i]);
      mapper.termidQuerySet.put(ToyCorpus.query[i], t);
    }
    mapper.queryTermidSet.put(1, query);   
  }
  
  private void lm_mapper(int termids[], String did, double res) throws Exception {
    PForDocVector.toIntArrayWritable(iaw, termids, termids.length);
    mapper.map(new Text(did), iaw, mapContext);
    keyOut.set(1, did); 
    valueOut.set( (float) res);
    verify(mapContext).write(keyOut, valueOut);    
  }

  @Test
  public void lm_mapper_d1() throws Exception {
    lm_mapper(ToyCorpus.d1, "d1", Math.log(0.05555555));
  }
  @Test
  public void lm_mapper_d2() throws Exception {
    lm_mapper(ToyCorpus.d2, "d2", Math.log(0.0625));
  }

  @Test
  public void lm_mapper_d3() throws Exception {
    lm_mapper(ToyCorpus.d3, "d3", Math.log(0.0416666666666));
  }
  
  @Test
  public void lm_mapper_d4() throws Exception {
    lm_mapper(ToyCorpus.d4, "d4", Math.log(0.08333333333333));
  }

  @Test
  public void lm_mapper_d6() throws Exception {
    lm_mapper(ToyCorpus.d6, "d6", Math.log(0.208333333));
  }
}
