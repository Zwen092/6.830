package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.index.BTreeLeafPage;
import simpledb.index.BTreePageId;
import simpledb.index.BTreeRootPtrPage;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;
import java.util.concurrent.Semaphore;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    final File f;
    final TupleDesc td;
    //private int totalPages;


    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
        //  totalPages = 0;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    //read pageSize bytes starting from _offset in File
    public Page readPage(PageId pid) {
        // some code goes here

        final int pageSize = BufferPool.getPageSize();
        final int offset = pid.getPageNumber() * pageSize;
        final byte[] pageBuffer = new byte[pageSize];

        HeapPage heapPage = null;
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(f);
            if (offset > 0) {
                fis.skip(offset);
            }

            if (fis.available() > 0) {
                if (fis.read(pageBuffer, 0, pageSize) <= 0) {
                    Debug.log("Failed to read page:" + pid.getPageNumber());
                    //return null;
                } else {
                    heapPage = new HeapPage((HeapPageId) pid, pageBuffer);
                }
            }
        } catch (IOException e) {
            Debug.log("HeapFile##readPage: " +
                    "Exception happened when database trying to get page from disk");
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("Failed to close this file stream");
                }
            }
        }
        return heapPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        PageId pageId = page.getId();
        int pageNo = pageId.getPageNumber();
        int offset = pageNo * BufferPool.getPageSize();
        byte[] pageData = page.getPageData();

        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        raf.seek(offset);
        raf.write(pageData);
        raf.close();

        page.markDirty(false, null);

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        long currentPageNo = (f.length() - 1 + BufferPool.getPageSize())
                / BufferPool.getPageSize();
        return (int) currentPageNo;

        //
    }

    public int numPages2() {
        return (int) f.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> pagesModified = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            PageId pageId = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            if (page.getNumEmptySlots() <= 0) {
                Database.getBufferPool().unsafeReleasePage(tid, pageId);
            } else {
                page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
                page.markDirty(true, tid);
                page.insertTuple(t);
                pagesModified.add(page);
                return pagesModified;

            }
        }

        //no more space in the existed pages or the are no page
        //create new page class and add it to the physical file
        HeapPageId pageId = new HeapPageId(getId(), numPages());
        byte[] emptyPage = new byte[BufferPool.getPageSize()];
        HeapPage newPage = new HeapPage(pageId, emptyPage);
        newPage.markDirty(true, tid);
        newPage.insertTuple(t);

        writePage(newPage);
        pagesModified.add(newPage);
        return pagesModified;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> ans = new ArrayList<>();
        PageId pageId = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);
        page.markDirty(true, tid);
        ans.add(page);
        return ans;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        //return new HeapFileIterator(this, tid);
        return new HeapFileIterator1(numPages(), tid, Permissions.READ_ONLY);
    }

    private class HeapFileIterator extends AbstractDbFileIterator {
        Iterator<Tuple> it = null;
        HeapPage currentPage = null;

        int pgNo = 0;

        HeapPageId currentPageId;
        final TransactionId transactionId;
        final HeapFile heapFile;

        /**
         * Constructor for this iterator
         *
         * @param heapFile      - the heapFile containing the tuples
         * @param transactionId - the transaction id
         */
        public HeapFileIterator(HeapFile heapFile, TransactionId transactionId) {
            this.heapFile = heapFile;
            this.transactionId = transactionId;
            this.currentPageId = new HeapPageId(heapFile.getId(), pgNo);
        }


        /**
         * Just read the next tuple from the currentPage
         *
         * @return
         * @throws DbException
         * @throws TransactionAbortedException
         */
        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            //read all tuples in current page
            if (it != null && !it.hasNext()) {
                HeapPageId nextId = new HeapPageId(heapFile.getId(), ++pgNo);
                if (pgNo == heapFile.numPages()) {
                    return null;
                }
                currentPage = (HeapPage) Database.getBufferPool().getPage(transactionId, nextId, Permissions.READ_ONLY);
                if (currentPage != null) {
                    it = currentPage.iterator();
                } else {
                    it = null;
                }

            }
            if (it == null)
                return null;
            return it.next();
        }


        /**
         * Initiate a iterator points to the first page
         *
         * @throws DbException
         * @throws TransactionAbortedException
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            HeapPage currentPage = (HeapPage) Database.getBufferPool().getPage(transactionId, currentPageId, Permissions.READ_ONLY);
            it = currentPage.iterator();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            super.close();
            it = null;
            currentPage = null;
        }
    }


    private class HeapFileIterator1 extends AbstractDbFileIterator {

        private TransactionId tid;
        private Permissions permissions;

        HeapFileIterator1(int pageNo, TransactionId tid, Permissions permissions) {
            this.pageNo = pageNo;
            this.tid = tid;
            this.permissions = permissions;
        }

        private int pageNo;
        private int iterIndex = 0;
        private Iterator<Tuple> iterator;

        @Override
        public void open() throws DbException, TransactionAbortedException {
            iterIndex = 0;
            iterator = this.getHeapPageIterator(iterIndex);
            if (iterator == null) {
                throw new DbException("iterator is null");
            }
        }

        private Iterator<Tuple> getHeapPageIterator(int pageNo)
                throws DbException, TransactionAbortedException {
            PageId pageId = new HeapPageId(getId(), pageNo);
            /*
                这里获取页面的权限值得商讨
                1. 如果使用READ_ONLY, 会导致并发事务的系统单元测试挂掉，无解
                2. 使用READ_WRITE，抢占该页数据，避免并发写覆盖的情况，为了通过单元测试。
             */
            Page page = Database.getBufferPool().getPage(tid, pageId, permissions);
            return ((HeapPage) page).iterator();
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (iterator == null || iterIndex >= pageNo) {
                return null;
            }
            while (!iterator.hasNext()) {
                iterIndex++;
                if (iterIndex < pageNo) {
                    iterator = this.getHeapPageIterator(iterIndex);
                } else {
                    break;
                }
            }

            if (iterIndex == pageNo) {
                return null;
            } else {
                return iterator.next();
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            super.close();
            this.iterIndex = pageNo;
        }
    }


}

