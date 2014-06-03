package org.clueweb.clueweb12.app;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
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

public class RMModelTest {
    private RMModel.MyMapper mapper;
  private Context mapContext;
  private IntArrayWritable iaw;
  
  @Before 
  public void setUp() {
    mapper = new RMModel.MyMapper();
    mapper.dictionary = mock(DefaultFrequencySortedDictionary.class);
    for(int i=0; i<=11; i++) {
      when(mapper.dictionary.getTerm(i)).thenReturn("t"+i);
    }
    
    mapper.stats = mock(org.clueweb.data.TermStatistics.class);
    for(int i=0; i<ToyCorpus.df.length; i++) {
      when(mapper.stats.getDf(i)).thenReturn(ToyCorpus.df[i]);
    }
    when(mapper.stats.getCollectionSize()).thenReturn((long) ToyCorpus.numDocs);
    
    mapContext = mock(Context.class);  
    iaw = new IntArrayWritable();
    mapper.smoothingParam = 0.5;
    mapper.numFeedbackDocs = 3;
    mapper.topRankedDocs = Sets.newHashSet();
    mapper.topRankedDocs.add("d4");
    mapper.topRankedDocs.add("d6");
    mapper.topRankedDocs.add("d5");


    mapper.qlRanking = new ArrayList<TRECResult>();
    mapper.qlRanking.add(new TRECResult(1, "d6", 1, 0.20833333, "dummy"));
    mapper.qlRanking.add(new TRECResult(1, "d4", 2, 0.08333333, "dummy"));
    mapper.qlRanking.add(new TRECResult(1, "d5", 2, 0.07291666, "dummy"));

  }
  
  @Test
  public void rmm_mapper_d4() throws Exception {
 
    PForDocVector.toIntArrayWritable(iaw, ToyCorpus.d4, ToyCorpus.d4.length);
    mapper.valueOut = mock(PairOfStringFloat.class);
    mapper.map(new Text("d4"), iaw, mapContext);
    
    //d4: 8,4,9
    double weight = 0.08333333;
    double p8 = (1d/3d * 1d/2d + 2d/6d * 1d/2d) * weight;
    double p4 = (1d/3d * 1d/2d + 2d/6d * 1d/2d) * weight;
    double p9 = (1d/3d * 1d/2d + 1d/6d * 1d/2d) * weight;
    
    InOrder inOrder = inOrder(mapper.valueOut, mapContext);
    
    inOrder.verify(mapper.valueOut).set("t4", (float)p4);
    inOrder.verify(mapper.valueOut).set("t8", (float)p8);
    inOrder.verify(mapper.valueOut).set("t9", (float)p9);


  }
}
