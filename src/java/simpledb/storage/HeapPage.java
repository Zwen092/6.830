package simpledb.storage;

import jdk.nashorn.internal.runtime.RewriteException;
import org.omg.IOP.TAG_ORB_TYPE;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Catalog;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    public final byte[] header;
    final Tuple[] tuples;
    final int numSlots;
    private TransactionId transactionId;
    private boolean isDirty;


    byte[] oldData;
    private final Byte oldDataLock= (byte) 0;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();
        
        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */

    private int getNumTuples() {
        return Math.floorDiv(BufferPool.getPageSize() * 8, (td.getSize() * 8 + 1));
    }



    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    public int getHeaderSize() {
        return (int)Math.ceil(getNumTuples() / 8.0);
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        RecordId recordId = t.getRecordId();
        PageId pageId = recordId.getPageId();
        int tupleNo = recordId.getTupleNumber();
        if (!pageId.equals(pid) || !isSlotUsed(tupleNo)) {
            throw new DbException("This tuple is not on this page or tuple slot is already empty");
        }
        //make the slot from filled to empty -> markSlotUsed(int tupleNo, boolean value)
        //make the corresponding tuple[tupleNo] -> null
        markSlotUsed(tupleNo, false);
        tuples[tupleNo] = null;

    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        TupleDesc tupleDesc = t.getTupleDesc();
        if (!tupleDesc.equals(td) || getNumEmptySlots() == 0) {
            throw new DbException("the page is full or tupleDesc mismatch");
        }
//        int tupleNo = t.getRecordId().getTupleNumber();
//        RecordId newId = new RecordId(this.pid, getNumTuples() - getNumEmptySlots());
//        t.setRecordId(newId);
//        markSlotUsed(tupleNo, true);
//        tuples[tupleNo] = t;
        for (int i = 0; i < getNumTuples(); i++) {
            if (!isSlotUsed(i)) {
                markSlotUsed(i, true);
                t.setRecordId(new RecordId(this.pid, i));
                //this.getNumTuples() - this.getNumEmptySlots()
                tuples[i] = t;
                return;
            }
        }
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
//        isDirty = dirty;
//        transactionId = tid;
        if (dirty) {
            transactionId = tid;
        } else {
            transactionId = null;
        }
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        //return isDirty ? transactionId : null;
        return transactionId;
    }

    public int getNumSlots() {
        return numSlots;
    }
    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        //iterate through the header to see how many bits are 0
        int total = 0;
        for (int i = 0; i < header.length; i++) {
            total += getZero(header[i]);
        }
        return total;
    }

    private int getZero(byte a) {
        int total = 0;
        for (int i = 0; i < 8; i++) {
            if (a % 2 == 0) total++;
            a >>= 1;
        }
        return total;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        //504 - 50 >= 484? false
//        int total = 0;
//        for (int j = 0; j < header.length; j++) {
//            total += 8 - getZero(header[j]);
//        }
//        return i < total;
        int quotient = i / 8;
        int remainder = i % 8;
        return ((header[quotient]&(1<<remainder))>>remainder) == 1;
    }




    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        /**
         * first we should find which header we should modify
         * i = 20
         * i / 8 = 2...4 the 2ed header
         * we set 00001111 to 00000111
         * which is 31 -> 15
         * or we set 00001111 -> 00001101
         * 00001111 &~(1<<x)
         * which is 31 -> 8 + 4 + 1 = 13
         *
         *
         */
        //find bits
        int quotient = i / 8;
        int remainder = i % 8;
        if (!value) {
            //set bit to 0
            int mask = ~(1 << remainder);
            header[quotient] &= mask;
        } else {
            //set bit to 1
            int mask = (1 << remainder);
            header[quotient] |= mask;
        }

    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        List<Tuple> tupleList = new ArrayList<>();
        for (Tuple tuple : tuples) {
            if (tuple != null) {
                tupleList.add(tuple);
            }
        }

        return tupleList.iterator();
    }


}

