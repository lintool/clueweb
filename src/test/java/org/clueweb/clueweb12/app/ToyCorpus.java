package org.clueweb.clueweb12.app;

public class ToyCorpus {
  public static int d1[] = {1,2,1,3,4,5};
  public static int d2[] = {1,5,6,2};
  public static int d3[] = {7,2};
  public static int d4[] = {8,4,9};
  public static int d5[] = {2,10,11};
  public static int d6[] = {8,5};
  
  public static int query[] = {5,8};
  
  //number of documents a term occurs in df[1]=2 means that term "1" appears in 2 documents;
  //we do not have a term 0
  public static int df[] = {0,2,4,1,2,3,1,1,2,1,1,1};
  
  public static int numDocs = 6;
}
