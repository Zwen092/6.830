package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.dsig.spec.DigestMethodParameterSpec;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    public final Map<Integer, IntHistogram> intHistograms = new HashMap<>();
    private final Map<Integer, StringHistogram> stringHistograms = new HashMap<>();
    private int numTuples;
    private int scannedTuples;
    private int numPages;
    public int scannedPages;
    private int ioCostPerPage;
    private TupleDesc tupleDesc;





    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here


        /**
         * scan the table multiple times, and build the histogram for every field in tupleDesc
         * and i need to count down how many pages I scanned
         */
        this.ioCostPerPage = ioCostPerPage;
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableid);
        TransactionId transactionId = new TransactionId();
        numPages = ((HeapFile)dbFile).numPages();
        //seems no need to get the dbFile
        SeqScan scan = new SeqScan(transactionId, tableid);
        tupleDesc = scan.getTupleDesc();
        int numFields = tupleDesc.numFields();
        int[] maxs = new int[numFields];
        int[] mins = new int[numFields];

        try {
            scan.open();
            for (int i = 0; i < numFields; i++) {
                if (tupleDesc.getFieldType(i) == Type.STRING_TYPE) {
                    continue;
                }
                int min = Integer.MAX_VALUE;
                int max = Integer.MIN_VALUE;
                //count the max and min of this field
                while (scan.hasNext()) {
                    if (i == 0) numTuples++;
                    Tuple tuple = scan.next();
                    IntField field = (IntField) tuple.getField(i);
                    int val = field.getValue();
                    if (val > max) max = val;
                    if (val < min) min = val;
                    scannedTuples++;
                }
                mins[i] = min;
                maxs[i] = max;
                scan.rewind();
            }
            scan.close();
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < numFields; i++) {
            Type type = tupleDesc.getFieldType(i);
            if (type == Type.INT_TYPE) {
                IntHistogram intHistogram = new IntHistogram(NUM_HIST_BINS, mins[i], maxs[i]);
                intHistograms.put(i, intHistogram);
            } else {
                StringHistogram stringHistogram = new StringHistogram(NUM_HIST_BINS);
                stringHistograms.put(i, stringHistogram);
            }
        }

        //add value to the corresponding histogram
        try {
            scan.open();
            while (scan.hasNext()) {
                Tuple tuple = scan.next();
                for (int i = 0; i < numFields; ++i) {
                    Field field = tuple.getField(i);
                    if (field.getType() == Type.INT_TYPE) {
                        int val = ((IntField) field).getValue();
                        intHistograms.get(i).addValue(val);
                    } else {
                        String val = ((StringField) field).getValue();
                        stringHistograms.get(i).addValue(val);
                    }
                }
                scannedTuples++;
            }
            scan.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        int pageSize = BufferPool.getPageSize();
        scannedPages = ((dbFile.getTupleDesc().getSize() * 8 + 1) * scannedTuples + (pageSize - 1) * 8) / (pageSize * 8);
    }


    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return this.ioCostPerPage * numPages;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int)(numTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        Type type = tupleDesc.getFieldType(field);
        if (type == Type.INT_TYPE) {
            return intHistograms.get(field).estimateSelectivity(op, ((IntField)constant).getValue());
        } else {
            return stringHistograms.get(field).estimateSelectivity(op, ((StringField)constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        return numTuples;
    }

}
