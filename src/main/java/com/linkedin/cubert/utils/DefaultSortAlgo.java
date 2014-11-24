/**
 * If no sort algorithm is specified in the script, this will be used.
 *
 * @author Vinitha Gankidi
 *
 */

package com.linkedin.cubert.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class DefaultSortAlgo<T> implements SortAlgo<T> {

  @Override
  public void sort(List<T> a, Comparator<? super T> c) {
    Collections.sort(a, c);
  }

}
