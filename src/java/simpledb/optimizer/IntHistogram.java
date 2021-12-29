package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.awt.*;
import java.util.ArrayList;
import java.util.List;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {


    private final ArrayList<Integer>[] buckets;
    private final int min;
    private final int max;
    private final int bucketRange;
    private int numTuples;

    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.buckets = new ArrayList[buckets];
        for (int i = 0; i < buckets; i++) {
            this.buckets[i] = new ArrayList();
        }
        this.min = min;
        this.max = max;
        if (max - min > buckets) {
            bucketRange = (max - min + 1) / buckets;
        } else {
            bucketRange = 1;
        }

        numTuples = 0;
    	// some code goes here
    }

    private int chooseBucket(int v) {
        /**
         * min = 1, max = 10; buckets = 5; 12 34 56 78 910 -> bucket 0, 1, 2, 3, 4; bucketRange = 2
         * 6 / 2 =3;
         * 5 / 2 = 2;
         * -60 -56 -55 -51; ....; -20 -16 -15 -11;
         */
        int index = (v - min) / bucketRange;
        if (index < 0) {
            index = 0;
        } else if (index > buckets.length - 1) {
            index = buckets.length - 1;
        }
        return index;
//
//        if (distance % bucketRange == 0) {
//            return distance / bucketRange - 1;
//        } else {
//            return distance / bucketRange;
//        }
    }
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if (v > max || v < min) return;
    	int index = chooseBucket(v);
    	buckets[index].add(v);
    	numTuples++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        switch (op) {
            case EQUALS: return eq(v);
            case GREATER_THAN: return GreaterThan(v);
            case LESS_THAN: return LessThan(v);
            case GREATER_THAN_OR_EQ: return GreaterThanOrEquals(v);
            case LESS_THAN_OR_EQ: return LessThanOrEquals(v);
            case NOT_EQUALS: return NotEquals(v);
            default: break;
        }
        return 0.0;

    }
    private double eq(int v) {
        if (v < min || v > max) {
            return 0;
        }
        int index = chooseBucket(v);
        return (double)buckets[index].size()/bucketRange/numTuples;
    }

    private double GreaterThan(int v) {
        /**
         * v=3, bright = 4
         * idx = 1, bright = (1 + 1) * 2
         *
         * v=3
         * idx =2,
         * bright = (2 + 1) * 1;
         *
         *
         */
        if (v < min) {
            return 1;
        }
        if (v > max) {
            return 0;
        }
       int index = chooseBucket(v);
       int bRight = (index + 1) * bucketRange;
       double s1 = (double) buckets[index].size() / numTuples * (bRight - v) / bucketRange;
       double s2 = 0;
       for (int i = index + 1; i < buckets.length; i++) {
           //System.out.println(buckets[i].size());
           s2 += (double) buckets[i].size() / numTuples;
       }
       return  (s1 + s2);

    }

    private double LessThan(int v) {
        if (v < min) {
            return 0;
        }
        if (v > max) {
            return 1;
        }
        int index = chooseBucket(v);
        int bLeft = index * bucketRange;
        double s1 = (double) buckets[index].size() / numTuples * (v - bLeft) / bucketRange;
        double s2 = 0;
        for (int i = index - 1; i >= 0; i--) {
            //System.out.println(buckets[i].size());
            s2 += (double) buckets[i].size() / numTuples;
        }
        return (s1 + s2);
    }

    private double GreaterThanOrEquals(int v) {
        if (v <= min) {
            return 1;
        }
        if (v > max) {
            return 0;
        }
        return GreaterThan(v) + eq(v);
    }

    private double LessThanOrEquals(int v) {
        if (v < min) {
            return 0;
        }
        if (v >= max) {
            return 1;
        }
        return LessThan(v) + eq(v);
    }

    private double NotEquals(int v) {
        return 1.0 - eq(v);
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }

    public void iterate() {
        for (int i = 0; i < buckets.length; i++) {
            for (int j : buckets[i]) {
                System.out.print("buckets[i]: " + j);
                System.out.println();
            }
        }
    }
}
