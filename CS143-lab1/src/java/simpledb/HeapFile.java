package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return null;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return 0;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    /**
     * Returns an iterator over all the tuples stored in this DbFile. The
     * iterator must use {@link BufferPool#getPage}, rather than
     * {@link #readPage} to iterate through the pages.
     *
     * @return an iterator over all the tuples stored in this DbFile.
     */
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        class DBterator implements DbFileIterator{
            private int index;
            private HeapFile HF;
            private TransactionId TID;
            private Iterator<Tuple> IT;

            public DBterator(HeapFile hf, TransactionId tid){
                TID = tid;
                HF = hf;
                index = 0;
                IT = null;
            }
            @Override
            //returns next tuple, needs tuple iterator
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(IT != null){
                    Tuple toReturn = null;//check for another tuple
                    if(this.hasNext()) {
                        toReturn = IT.next();//gets next tuple from buffer page
                        return toReturn;
                    }
                    else
                        throw new NoSuchElementException();
                }
                //if IT is null
                else{
                    throw new NoSuchElementException();
                }

            }

            @Override
            public void close() {
                IT = null;
            }

            @Override
            //starts the iterator.
            public void open() throws DbException, TransactionAbortedException {
                //open creates the iterator
                HeapPage HP;// use getPage(TransactionId tid, PageId pid, Permissions perm)
                //page id = from heap file
                //page no?
                //get bufferpool from database class
                HeapPageId ID = new HeapPageId(HF.getId(),index);
                HP = ((HeapPage)(Database.getBufferPool().getPage(TID, ID, Permissions.READ_ONLY)));
                IT = HP.iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (IT == null){
                    //iterator hasn't bee=n declared so open
                   return false;
                }
                else{//iT declared
                    //check if there is a tuple!
                    if (IT.hasNext())
                        return true;
                    else
                        return false;
                }

            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                //rewinds the iterator back to beginning
                index = 0;
                open();
            }
        }
        DBterator temp = new DBterator(this, tid);
        return temp;
    }

}

