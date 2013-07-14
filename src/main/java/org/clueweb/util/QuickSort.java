/*
 * ClueWeb Tools: Hadoop tools for manipulating ClueWeb collections
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.clueweb.util;

public class QuickSort {

    // quicksort a[left] to a[right]
    public static void quicksort(short[] keys, int[] counts, int left, int right) {
        if (right <= left) return;
        int i = partition(keys, counts, left, right);
        quicksort(keys, counts, left, i-1);
        quicksort(keys, counts, i+1, right);
    }
 
 // quicksort a[left] to a[right]
    public static void quicksort(int[] keys, int[] counts, short[] counts2, int left, int right) {
        if (right <= left) return;
        int i = partition(keys, counts, counts2, left, right);
        quicksort(keys, counts, counts2, left, i-1);
        quicksort(keys, counts, counts2, i+1, right);
    }
    
    
    public static void quicksort(short[] keys, int[] counts, Object[] counts2, int left, int right) {
        if (right <= left) return;
        int i = partition(keys, counts, counts2, left, right);
        quicksort(keys, counts, counts2, left, i-1);
        quicksort(keys, counts, counts2, i+1, right);
    }
    
    public static void quicksortWithSecondary(int[] keys, int[] counts, short[] counts2, int left, int right) {
        if (right <= left) return;
        int i = partitionWithSecondary(keys, counts, counts2, left, right);
        quicksortWithSecondary(keys, counts, counts2, left, i-1);
        quicksortWithSecondary(keys, counts, counts2, i+1, right);
    }
    
    public static void quicksortWithSecondary(int[] keys, int[] counts, long[] counts2, int left, int right) {
        if (right <= left) return;
        int i = partitionWithSecondary(keys, counts, counts2, left, right);
        quicksortWithSecondary(keys, counts, counts2, left, i-1);
        quicksortWithSecondary(keys, counts, counts2, i+1, right);
    }
    
    public static void quicksort(int[] keys, int[] counts, int left, int right) {
        if (right <= left) return;
        int i = partition(keys, counts, left, right);
        quicksort(keys, counts, left, i-1);
        quicksort(keys, counts, i+1, right);
    }
    
    public static void quicksort(Object[] keys, int[] counts, int left, int right) {
        if (right <= left) return;
        int i = partition(keys, counts, left, right);
        quicksort(keys, counts, left, i-1);
        quicksort(keys, counts, i+1, right);
    }

    // partition a[left] to a[right], assumes left < right
    private static int partition(short[] keys, int[] counts, int left, int right) {
        int i = left - 1;
        int j = right;
        while (true) {
            while (counts[++i] < counts[right])      // find item on left to swap
                ;                               // a[right] acts as sentinel
            while (counts[right] < counts[--j])      // find item on right to swap
                if (j == left) break;           // don't go out-of-bounds
            if (i >= j) break;                  // check if pointers cross
            int swap = counts[i];
            counts[i] = counts[j];
            counts[j] = swap;                      // swap two elements into place
            
            short tmp = keys[i];
            keys[i] = keys[j];
            keys[j] = tmp; 
        }
        int swap = counts[i];
        counts[i] = counts[right];
        counts[right] = swap;
        short tmp = keys[i];
        keys[i] = keys[right];
        keys[right] = tmp;
        return i;
    }
    
    private static int partition(Object[] keys, int[] counts, int left, int right) {
        int i = left - 1;
        int j = right;
        while (true) {
            while (counts[++i] < counts[right])      // find item on left to swap
                ;                               // a[right] acts as sentinel
            while (counts[right] < counts[--j])      // find item on right to swap
                if (j == left) break;           // don't go out-of-bounds
            if (i >= j) break;                  // check if pointers cross
            int swap = counts[i];
            counts[i] = counts[j];
            counts[j] = swap;                      // swap two elements into place
            
            Object tmp = keys[i];
            keys[i] = keys[j];
            keys[j] = tmp; 
        }
        int swap = counts[i];
        counts[i] = counts[right];
        counts[right] = swap;
        Object tmp = keys[i];
        keys[i] = keys[right];
        keys[right] = tmp;
        return i;
    }
    
    private static int partition(int[] keys, int[] counts, short[] counts2, int left, int right) {
        int i = left - 1;
        int j = right;
        while (true) {
            while (counts[++i] < counts[right])      // find item on left to swap
                ;                               // a[right] acts as sentinel
            while (counts[right] < counts[--j])      // find item on right to swap
                if (j == left) break;           // don't go out-of-bounds
            if (i >= j) break;                  // check if pointers cross
            int swap = counts[i];
            counts[i] = counts[j];
            counts[j] = swap;                      // swap two elements into place
            
            int tmp = keys[i];
            keys[i] = keys[j];
            keys[j] = tmp; 
            
            short tmp2 = counts2[i];
            counts2[i] = counts2[j];
            counts2[j] = tmp2;
            
        }
        int swap = counts[i];
        counts[i] = counts[right];
        counts[right] = swap;
        
        int tmp = keys[i];
        keys[i] = keys[right];
        keys[right] = tmp;
        
        short tmp2 = counts2[i];
        counts2[i] = counts2[right];
        counts2[right] = tmp2;
        return i;
    }
    
    private static int partition(short[] keys, int[] counts, Object[] counts2, int left, int right) {
        int i = left - 1;
        int j = right;
        while (true) {
            while (counts[++i] < counts[right])      // find item on left to swap
                ;                               // a[right] acts as sentinel
            while (counts[right] < counts[--j])      // find item on right to swap
                if (j == left) break;           // don't go out-of-bounds
            if (i >= j) break;                  // check if pointers cross
            int swap = counts[i];
            counts[i] = counts[j];
            counts[j] = swap;                      // swap two elements into place
            
            short tmp = keys[i];
            keys[i] = keys[j];
            keys[j] = tmp; 
            
            Object tmp2 = counts2[i];
            counts2[i] = counts2[j];
            counts2[j] = tmp2;
            
        }
        int swap = counts[i];
        counts[i] = counts[right];
        counts[right] = swap;
        
        short tmp = keys[i];
        keys[i] = keys[right];
        keys[right] = tmp;
        
        Object tmp2 = counts2[i];
        counts2[i] = counts2[right];
        counts2[right] = tmp2;
        return i;
    }
    
    private static int partitionWithSecondary(int[] keys, int[] counts, short[] counts2, int left, int right) {
        int i = left - 1;
        int j = right;
        while (true) {
          do{
            i++;
          }while (counts[i] < counts[right] || (counts[i] == counts[right] && keys[i] < keys[right]))      // find item on left to swap
                ;                               // a[right] acts as sentinel
          
            //while (counts[++i] < counts[right])      // find item on left to swap
            //    ;                               // a[right] acts as sentinel
            do{
              j--;
            }while (j!=left && (counts[right] < counts[j] || (counts[right] == counts[j] && keys[right]<keys[j] )));      // find item on right to swap
          
          //while (counts[right] < counts[--j])      // find item on right to swap
            //    if (j == left) break;           // don't go out-of-bounds
            if (i >= j) break;                  // check if pointers cross
            int swap = counts[i];
            counts[i] = counts[j];
            counts[j] = swap;                      // swap two elements into place
            
            int tmp = keys[i];
            keys[i] = keys[j];
            keys[j] = tmp; 
            
            short tmp2 = counts2[i];
            counts2[i] = counts2[j];
            counts2[j] = tmp2;
            
        }
        int swap = counts[i];
        counts[i] = counts[right];
        counts[right] = swap;
        
        int tmp = keys[i];
        keys[i] = keys[right];
        keys[right] = tmp;
        
        short tmp2 = counts2[i];
        counts2[i] = counts2[right];
        counts2[right] = tmp2;
        return i;
    }
    
    private static int partitionWithSecondary(int[] keys, int[] counts, long[] counts2, int left, int right) {
        int i = left - 1;
        int j = right;
        while (true) {
          do{
            i++;
          }while (counts[i] < counts[right] || (counts[i] == counts[right] && keys[i] < keys[right]))      // find item on left to swap
                ;                               // a[right] acts as sentinel
          
            //while (counts[++i] < counts[right])      // find item on left to swap
            //    ;                               // a[right] acts as sentinel
            do{
              j--;
            }while (j!=left && (counts[right] < counts[j] || (counts[right] == counts[j] && keys[right]<keys[j] )));      // find item on right to swap
          
          //while (counts[right] < counts[--j])      // find item on right to swap
            //    if (j == left) break;           // don't go out-of-bounds
            if (i >= j) break;                  // check if pointers cross
            int swap = counts[i];
            counts[i] = counts[j];
            counts[j] = swap;                      // swap two elements into place
            
            int tmp = keys[i];
            keys[i] = keys[j];
            keys[j] = tmp; 
            
            long tmp2 = counts2[i];
            counts2[i] = counts2[j];
            counts2[j] = tmp2;
            
        }
        int swap = counts[i];
        counts[i] = counts[right];
        counts[right] = swap;
        
        int tmp = keys[i];
        keys[i] = keys[right];
        keys[right] = tmp;
        
        long tmp2 = counts2[i];
        counts2[i] = counts2[right];
        counts2[right] = tmp2;
        return i;
    }
    
    private static int partition(int[] keys, int[] counts, int left, int right) {
        int i = left - 1;
        int j = right;
        while (true) {
            while (counts[++i] < counts[right])      // find item on left to swap
                ;                               // a[right] acts as sentinel
            while (counts[right] < counts[--j])      // find item on right to swap
                if (j == left) break;           // don't go out-of-bounds
            if (i >= j) break;                  // check if pointers cross
            int swap = counts[i];
            counts[i] = counts[j];
            counts[j] = swap;                      // swap two elements into place
            
            int tmp = keys[i];
            keys[i] = keys[j];
            keys[j] = tmp; 
        }
        int swap = counts[i];
        counts[i] = counts[right];
        counts[right] = swap;
        
        int tmp = keys[i];
        keys[i] = keys[right];
        keys[right] = tmp;
        
        return i;
    }
}
