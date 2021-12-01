package simpledb;

import simpledb.common.Database;
import simpledb.common.Utility;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import junit.framework.JUnit4TestAdapter;
import simpledb.transaction.TransactionId;

public class HeapFileReadTest extends SimpleDbTestBase {
    private HeapFile hf;
    private TransactionId tid;
    private TupleDesc td;

    /**
     * Set up initial resources for each unit test.
     */
    @Before
    public void setUp() throws Exception {

        hf = SystemTestUtil.createRandomHeapFile(2, 20, null, null);
        td = Utility.getTupleDesc(2);
        tid = new TransactionId();
    }

    @After
    public void tearDown() {
        Database.getBufferPool().transactionComplete(tid);
    }

    @Test
    public void myScanTest() throws Exception {

//        int[] columnSizes = new int[]{1, 2, 3, 4};
//        int[] rowSizes =
//                new int[]{1, 2, 511, 512, 513, 1023, 1024, 1025, 4096};
//
//
//        List<List<Integer>> tuples = new ArrayList<>();
//        HeapFile f = SystemTestUtil.createRandomHeapFile(10, 500000, null, tuples);
//        System.out.println(f.numPages());
//        System.out.println(f.numPages2());


        List<List<Integer>> tuples = new ArrayList<>();
        HeapFile f = SystemTestUtil.createRandomHeapFile(3, 2, null, tuples);
        HeapPageId pageId = new HeapPageId(f.getId(), 0);
        HeapPage page = (HeapPage) f.readPage(pageId);
        System.out.println(page.getNumSlots());
        System.out.println(page.getNumEmptySlots());
        System.out.println(page.isSlotUsed(0));

    }

    /**
     * Unit test for HeapFile.getId()
     */
    @Test
    public void getId() throws Exception {
        int id = hf.getId();

        // NOTE(ghuo): the value could be anything. test determinism, at least.
        assertEquals(id, hf.getId());
        assertEquals(id, hf.getId());

        HeapFile other = SystemTestUtil.createRandomHeapFile(1, 1, null, null);
        assertTrue(id != other.getId());
    }

    /**
     * Unit test for HeapFile.getTupleDesc()
     */
    @Test
    public void getTupleDesc() {
        assertEquals(td, hf.getTupleDesc());
    }

    /**
     * Unit test for HeapFile.numPages()
     */
    @Test
    public void numPages() {
        assertEquals(1, hf.numPages());
        // assertEquals(1, empty.numPages());
//        FileInputStream fis = null;
//
//        try {
//            fis = new FileInputStream(hf.getFile());
//
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
        System.out.println(hf.getFile().length());
        HeapPageId pid = new HeapPageId(hf.getId(), 100);
        hf.readPage(pid);
        System.out.println(hf.numPages());
    }

    /**
     * Unit test for HeapFile.readPage()
     */
    @Test
    public void readPage() {
        HeapPageId pid = new HeapPageId(hf.getId(), 0);
        HeapPage page = (HeapPage) hf.readPage(pid);

        // NOTE(ghuo): we try not to dig too deeply into the Page API here; we
        // rely on HeapPageTest for that. perform some basic checks.
        assertEquals(484, page.getNumEmptySlots());
        assertTrue(page.isSlotUsed(1));
        assertFalse(page.isSlotUsed(20));
    }

    @Test
    public void testIteratorBasic() throws Exception {
//        HeapFile smallFile = SystemTestUtil.createRandomHeapFile(2, 3, null,
//                null);

        HeapFile smallFile = SystemTestUtil.createRandomHeapFile(4, 700, null,
                null);

        DbFileIterator it = smallFile.iterator(tid);
        // Not open yet
        assertFalse(it.hasNext());
        try {
            it.next();
            fail("expected exception");
        } catch (NoSuchElementException ignored) {
        }

        it.open();
        int count = 0;
        //this iterator iterate through tuples
        while (it.hasNext()) {
            assertNotNull(it.next());
            count += 1;
        }

        it.close();
    }

    @Test
    public void testIteratorClose() throws Exception {
        // make more than 1 page. Previous closed iterator would start fetching
        // from page 1.
        HeapFile twoPageFile = SystemTestUtil.createRandomHeapFile(2, 520,
                null, null);
        DbFileIterator it = twoPageFile.iterator(tid);
        it.open();
        assertTrue(it.hasNext());
        it.close();
        try {
            it.next();
            fail("expected exception");
        } catch (NoSuchElementException ignored) {
        }
        // close twice is harmless
        it.close();
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(HeapFileReadTest.class);
    }
}
