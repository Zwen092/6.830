package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private static class dNode {
        PageId pageId;
        Page page;
        dNode prev;
        dNode next;
        dNode () {}
        dNode (PageId id, Page page) {
            this.pageId = id;
            this.page = page;
        }
    }

    private final Map<PageId, dNode> bufferPool = new ConcurrentHashMap<>();
    int size;
    dNode sentinel = new dNode();
    int numPages;


    //I may use a map to track the

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.size = 0;
        sentinel.next = sentinel;
        sentinel.prev = sentinel;
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }



    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        dNode node = bufferPool.get(pid);
        if (node == null) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
//            if (page != null) {
////                if (bufferPool.size() >= this.numPages) {
////                    this.evictPage();
////                }
//////                if (perm == Permissions.READ_WRITE) {
//////                    page.markDirty(true, tid);
//////                }
////                dNode newNode =
////                bufferPool.put(pid, page);
//            }
            node = new dNode(pid, page);
            bufferPool.put(pid, node);
            addToHead(node);
            size++;
            if (size > numPages) {
                evictPage();
            }
        }
        moveToHead(node);
        return node.page;
    }
    private void addToHead(dNode node) {
        node.prev = sentinel;
        node.next = sentinel.next;
        sentinel.next.prev = node;
        sentinel.next = node;
    }

    private void moveToHead(dNode node) {
        //move this node to head
        node.prev.next = node.next;
        node.next.prev = node.prev;
        node.prev = sentinel;
        node.next = sentinel.next;
        sentinel.next.prev = node;
        sentinel.next = node;
    }

    private dNode removeTail() {
        dNode tail = sentinel.prev;
        tail.prev.next = tail.next;
        sentinel.prev = tail.prev;
        return tail;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> dirtyPages = (ArrayList<Page>) Database.getCatalog().getDatabaseFile(tableId)
                .insertTuple(tid, t);

        for (Page dirtyPage : dirtyPages) {
            dirtyPage.markDirty(true, tid);
            dNode node = new dNode(dirtyPage.getId(), dirtyPage);
            this.bufferPool.put(dirtyPage.getId(), node);
            addToHead(node);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        RecordId recordId = t.getRecordId();
        PageId pageId = recordId.getPageId();
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(pageId.getTableId());
        heapFile.deleteTuple(tid, t);
//        for (Page page : pages) {
//            page.markDirty(true, tid);
//            bufferPool.put(pageId, new dNode(page.getId(), page));
//        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Map.Entry<PageId, dNode> group : bufferPool.entrySet()) {
            Page page = group.getValue().page;
            if (page.isDirty() != null) {
                this.flushPage(group.getKey());
            }
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        if (pid == null) {
            return;
        }
        bufferPool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page pageToBeFlushed = bufferPool.get(pid).page;
        TransactionId tid = pageToBeFlushed.isDirty();
        if (pageToBeFlushed != null && tid != null) {
            Page before = pageToBeFlushed.getBeforeImage();
            // flushPage本身无事务控制，不应该调用setBeforeImage
            // pageToBeFlushed.setBeforeImage();
            Database.getLogFile().logWrite(tid, before, pageToBeFlushed);
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pageToBeFlushed);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     *
     * I should evict the Least Recently Used page
     * every time the bufferPool interact with a page
     * the page should be to the front of this dlinkedList
     * when evict, evict the Least Recently Used page and flush it to disk if it is dirty
     */
    private synchronized void evictPage() throws DbException {

        dNode tail = removeTail();
        bufferPool.remove(tail.pageId);
        size--;
        if (tail.page.isDirty() != null) {
            try {
                flushPage(tail.pageId);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     *
     */
    private void put(PageId pageId, Page page) {

    }

}
