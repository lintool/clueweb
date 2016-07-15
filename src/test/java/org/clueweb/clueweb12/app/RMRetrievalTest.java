package org.clueweb.clueweb12.app;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.clueweb.clueweb12.app.RMRetrieval.RelLM;
import org.clueweb.data.PForDocVector;
import org.clueweb.dictionary.DefaultFrequencySortedDictionary;
import org.clueweb.util.TRECResult;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import tl.lin.data.array.IntArrayWritable;
import tl.lin.data.pair.PairOfIntString;
import tl.lin.data.pair.PairOfStringFloat;

/* 
 * Tests to evaluate the mapper of RMModel.
 * Toy corpus employed is the same as in RetrievalTest
 * 
 */

public class RMRetrievalTest {

  private RMRetrieval.MyMapper mapper;
  private Context mapContext;
  private IntArrayWritable iaw;
  private RelLM rellm;

  @Before
  public void setUp() {
    mapper = new RMRetrieval.MyMapper();
    mapper.dictionary = mock(DefaultFrequencySortedDictionary.class);

    for (int i = 0; i <= 11; i++) {
      when(mapper.dictionary.getTerm(i)).thenReturn("t" + i);
    }

    mapper.stats = mock(org.clueweb.data.TermStatistics.class);
    for (int i = 0; i < ToyCorpus.df.length; i++) {
      when(mapper.stats.getDf(i)).thenReturn(ToyCorpus.df[i]);
    }
    when(mapper.stats.getCollectionSize()).thenReturn((long) ToyCorpus.numDocs);

    mapContext = mock(Context.class);
    iaw = new IntArrayWritable();
    mapper.smoothingParam = 0.5;
    mapper.queryLambda = 0.0;

    // setup the relevance model
    mapper.relLMMap = Maps.newHashMap();// HashMap<Integer, RelLM> relLMMap;
    rellm = new RelLM(1);
    mapper.relLMMap.put(1, rellm);

    double p1 = 0.5 * (0.5 * 1d / 2d + 0.5 * 2d / 6d);
    double p2 = 0.5 * (0.5 * 1d / 2d + 0.5 * 3d / 6d) + 0.2 * (0.5 * 1d / 3d + 0.5 * 3d / 6d);
    double p6 = 0.2 * (0.5 * 1d / 3d + 0.5 * 3d / 6d);
    double p10 = 0.2 * (0.5 * 1d / 3d + 0.5 * 2d / 6d);

    // query 1 5 10
    p1 = (1f - mapper.queryLambda) * p1 + mapper.queryLambda * 1f / 3f;
    p10 = (1f - mapper.queryLambda) * p10 + mapper.queryLambda * 1f / 3f;

    rellm.addTerm(1, p1);
    rellm.addTerm(2, p2);
    rellm.addTerm(6, p6);
    rellm.addTerm(10, p10);
  }

  @Test
  public void rmretrieval_mapper_d2() throws Exception {

    int termids[] = { 1, 2 };
    PForDocVector.toIntArrayWritable(iaw, termids, termids.length);

    mapper.valueOut = mock(FloatWritable.class);
    mapper.map(new Text("d2"), iaw, mapContext);
    double p1 = 0.5 * (0.5 * 1d / 2d + 0.5 * 2d / 6d);
    double p2 = 0.5 * (0.5 * 1d / 2d + 0.5 * 3d / 6d);

//    InOrder inOrder = inOrder(mapper.valueOut, mapContext);
//    inOrder.verify(mapper.valueOut).set((float) p1);
    //inOrder.verify(mapper.valueOut).set((float) p2);

  }
}
