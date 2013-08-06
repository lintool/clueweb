package org.clueweb.util;

import java.util.Comparator;

import tl.lin.data.pair.PairOfStringFloat;

public class PairOfStringFloatComparator implements
    Comparator<PairOfStringFloat> {
  @Override
  public int compare(PairOfStringFloat o1, PairOfStringFloat o2) {

    if (o1.getRightElement() == o2.getRightElement()) {
      return 0;
    }
    if (o1.getRightElement() > o2.getRightElement()) {
      return 1;
    }
    return -1;
  }
}
