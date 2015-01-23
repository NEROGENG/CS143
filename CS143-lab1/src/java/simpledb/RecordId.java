package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    private PageId PID;
    private int Tuplenum;
    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // some code goes here
        PID = pid;
        Tuplenum = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        // some code goes here
        return Tuplenum;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return PID;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        //to represent the same tuple, it has to have the same page id and tuple no
        //cast object as record id
        try {
            if (((RecordId) o).getPageId() == this.getPageId()){
                if (((RecordId)o).tupleno() == this.tupleno())
                    return true;


            }
            //if neither if statement works, return false;
            return false;
        }catch (Exception e){
            //if it isn't a record number
            return false;
        }
        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");

        //has to be unique, unique being spot in pageID and tuple no
        //represent the page as number
        return (this.tupleno() + this.getPageId().getTableId());

    }

}
