package simpledb.execution;

import simpledb.common.Database;
import simpledb.storage.DbFile;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    final TransactionId transactionId;
    int tableId;
    String tableAlias;


    private DbFile dbFile;
    private DbFileIterator dbFileIterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableId
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableId, String tableAlias) {
        // some code goes here
        this.tableId = tableId;
        this.transactionId = tid;
        this.tableAlias = tableAlias;

        this.dbFile = Database.getCatalog().getDatabaseFile(tableId);
        this.dbFileIterator = dbFile.iterator(tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(this.tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableAlias = tableAlias;
        this.tableId = tableid;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        dbFileIterator.open();
    }


    @Override
    public boolean hasNext() throws TransactionAbortedException, DbException {
        return dbFileIterator.hasNext();
    }

    @Override
    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        return dbFileIterator.next();
    }

    @Override
    public void close() {
        dbFileIterator.close();
    }

    @Override
    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        dbFileIterator.rewind();
    }

//    public void open() throws DbException, TransactionAbortedException {
//        f = Database.getCatalog().getDatabaseFile(this.tableId);
//        it = f.iterator(this.transactionId);
//        it.open();
//    }
//
    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    //I do a deep copy and change or just a shallow copy?
    public TupleDesc getTupleDesc() {
        TupleDesc.TDItem[] tdItems = Database.getCatalog().getTupleDesc(this.tableId).getTdItems().clone();
        for (int i = 0; i < tdItems.length; i++) {
            tdItems[i] = new TupleDesc.TDItem(tdItems[i].fieldType, this.tableAlias + "." + tdItems[i].fieldName);
        }
        return new TupleDesc(tdItems);

    }
//
//
//
//    public boolean hasNext() throws TransactionAbortedException, DbException {
//        return it.hasNext();
//    }
//
//    public Tuple next() throws NoSuchElementException,
//            TransactionAbortedException, DbException {
//        return it.next();
//    }
//
//    public void close() {
//        it.close();
//    }
//
//    public void rewind() throws DbException, NoSuchElementException,
//            TransactionAbortedException {
//        it.rewind();
//    }
}
