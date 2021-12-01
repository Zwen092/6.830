package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId transactionId;
    private final OpIterator child;
    private final int tableId;
    private final TupleDesc tupleDesc;
    private boolean hasNoMoreElements;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.transactionId = t;
        this.child = child;
        this.tableId = tableId;
        this.hasNoMoreElements = false;

        Type[] types = new Type[1];
        types[0] = Type.INT_TYPE;
        this.tupleDesc = new TupleDesc(types);

    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        hasNoMoreElements = false;
    }

    public void close() {
        super.close();
        child.close();
        hasNoMoreElements = true;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {

//        if (!child.hasNext()) {
//            return null;
//        }
//        Type[] types = new Type[1];
//        types[0] = Type.INT_TYPE;
//        TupleDesc tupleDesc = new TupleDesc(types);
//        Tuple t = new Tuple(tupleDesc);
//        try {
//            Database.getBufferPool().insertTuple(transactionId, tableId, child.next());
//            t.setField(0, new IntField(1));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return t;
        if (hasNoMoreElements) {
            return null;
        }
        Tuple insertResult = new Tuple(tupleDesc);
        int records = 0;
        while (child.hasNext()) {
            try {
                Database.getBufferPool().insertTuple(transactionId, tableId, child.next());
                records++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        insertResult.setField(0, new IntField(records));
        hasNoMoreElements = true;
        return insertResult;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    }
}
