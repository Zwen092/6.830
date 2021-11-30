package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private static final IntField EMPTY_FIELD = new IntField(-1);

    private final int gbField;
    private final Type gbFieldType;
    private final int aField;
    private final Op what;

    /**
     * GroupFiled -> totalSum
     */
    private Map<Field, Integer> totalSum;

    /**
     * GroupFiled -> totalCounts
     */
    private Map<Field, Integer> totalCounts;

    /**
     * GroupFiled -> result
     */
    private Map<Field, Tuple> results;

    private TupleDesc tupleDescForAggregatorResult;


    /**
     * Aggregate constructor
     * aggregate by the gbField index and calculate the aField result
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.what = what;

        this.totalCounts = new ConcurrentHashMap<>();
        this.totalSum    = new ConcurrentHashMap<>();
        this.results = new ConcurrentHashMap<>();

        Type[] types;
        String[] names;
        if (this.gbField != NO_GROUPING) {
            types = new Type[2];
            names = new String[2];
            types[0] = this.gbFieldType;
            types[1] = Type.INT_TYPE;
            names[0] = "groupVal";
            names[1] = "aggregateVal";
        } else {
            types = new Type[1];
            names = new String[1];
            types[0] = Type.INT_TYPE;
            names[0] = "aggregateVal";
        }
        this.tupleDescForAggregatorResult = new TupleDesc(types, names);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field curGrpByField;
        if (this.gbField == NO_GROUPING) {
            curGrpByField = EMPTY_FIELD;
        } else {
            curGrpByField = tup.getField(this.gbField);
            if (curGrpByField.getType() != this.gbFieldType) {
                return;
            }
        }
        IntField curAgField = (IntField) tup.getField(this.aField);
        if (totalSum.containsKey(curGrpByField)) {
            totalSum.put(curGrpByField, totalSum.get(curGrpByField) + curAgField.getValue());
        } else {
            totalSum.put(curGrpByField, curAgField.getValue());
        }

        if (totalCounts.containsKey(curGrpByField)) {
            totalCounts.put(curGrpByField, totalCounts.get(curGrpByField) + 1);
        } else {
            totalCounts.put(curGrpByField, 1);
        }
        IntField after = null;
        if (!this.results.containsKey(curGrpByField)) {
            after = curAgField;
            if (what == Op.COUNT) {
                after = new IntField(1);
            }
        } else {
            //contain the current grouped field
            Tuple preResult = this.results.get(curGrpByField);
            // 拿到一个字段，即Aggregate的值
            final int indexForAgVal = preResult.getTupleDesc().numFields() - 1;
            IntField preAgField = (IntField) preResult.getField(indexForAgVal);
            switch (what){
                case MAX:
                    after = preAgField.getValue() < curAgField.getValue() ? curAgField : preAgField;
                    break;
                case MIN:
                    after = preAgField.getValue() < curAgField.getValue() ? preAgField : curAgField;
                    break;
                case COUNT:
                    after = new IntField(preAgField.getValue() + 1);
                    break;
                case SUM:
                    after = new IntField(preAgField.getValue() + curAgField.getValue());
                    break;
                case AVG:
                    after = new IntField(totalSum.get(curGrpByField) / totalCounts.get(curGrpByField)); // 会有精度损失
                    break;
                case SUM_COUNT: break;
                case SC_AVG: break;
                default:
                    break;
            }
        }

        if (after != null) {
            Tuple history = new Tuple(this.tupleDescForAggregatorResult);
            if (this.gbField >= 0) {
                history.setField(0, curGrpByField);
                history.setField(1, after);
            } else {
                history.setField(0, after);
            }
            this.results.put(curGrpByField, history);

        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new IntegerAgOpIterator();
    }

    private class IntegerAgOpIterator implements OpIterator {
        private boolean opened = false;
        private Iterator<Map.Entry<Field, Tuple>> iter;

        @Override
        public void open() throws DbException, TransactionAbortedException {
            iter = results.entrySet().iterator();
            opened = true;
        }

        @Override
        public void close() {
            opened = false;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return opened && iter.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return iter.next().getValue();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.close();
            this.open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return tupleDescForAggregatorResult;
        }
    }

}
