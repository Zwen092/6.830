package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private final int aggregateField;
    private final int groupField;
    private final Aggregator.Op aggregateOp;

    private Type aggregateType;
    private Type groupByType;

    private TupleDesc tupleDesc;
    private Aggregator aggregator;
    private OpIterator opIterator;



    /**
     * Constructor.
     * <p>
     *
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.aggregateField = afield;
        this.groupField = gfield;
        this.aggregateOp = aop;
        this.tupleDesc = child.getTupleDesc();

        //I should deal with no-grouping situation
        //that time, gField -> -1
        try {
            groupByType = this.groupField == -1 ? null : this.tupleDesc.getFieldType(gfield);
            //groupByType = this.tupleDesc.getFieldType(gfield);
            aggregateType = this.tupleDesc.getFieldType(afield);
        } catch (NoSuchElementException e) {
            groupByType = null;
            aggregateType = null;
        }

        if (aggregateType == Type.INT_TYPE) {
            this.aggregator = new IntegerAggregator(groupField, groupByType, afield, aop);
        } else {
            this.aggregator = new StringAggregator(groupField, groupByType, afield, aop);
        }



    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return this.groupField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        if (groupField == Aggregator.NO_GROUPING) {
            return null;
        } else {
            return groupByType.name();
        }
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return aggregateField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return aggregateType.name();
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aggregateOp;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        super.open();
        child.open();
        while (child.hasNext()) {
            Tuple nextTuple = child.next();
            aggregator.mergeTupleIntoGroup(nextTuple);
        }
        this.opIterator = aggregator.iterator();
        opIterator.open();


    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (opIterator.hasNext()) {
            return opIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        opIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        Type[] types;
        if (groupField == Aggregator.NO_GROUPING) {
            types = new Type[1];
            types[0] = aggregateType;
        } else {
            types = new Type[2];
            types[0] = groupByType;
            types[1] = aggregateType;
        }
        return new TupleDesc(types);

    }

    public void close() {
        super.close();
        child.close();
        opIterator.close();

    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }

}
