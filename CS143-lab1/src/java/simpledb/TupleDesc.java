package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    public List <TDItem> TDarray;
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldType + "(" + fieldName + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        //return null;
        return TDarray.listIterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        TDarray = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++){
            TDItem temp = new TDItem(typeAr[i], fieldAr[i]);
            TDarray.add(temp);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        TDarray = new ArrayList<TDItem>();
        //for the number of items in array
        for (int i = 0; i < typeAr.length; i++) {
            //create new item and add to array list
            TDItem temp = new TDItem(typeAr[i], "");
            TDarray.add(temp);
        }
        // some code goes here
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        //return 0;
        if (TDarray.isEmpty() || TDarray == null){
            return 0;
        }
        else
            return TDarray.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        //return null;
        //if index is invalid
        if (i < 0 || i > (TDarray.size()-1)){
            throw new NoSuchElementException();
        }
        else
            return TDarray.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        //return null;
        if (i <0 || i > (TDarray.size()-1)){
            throw new NoSuchElementException();
        }
        else
            return TDarray.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        //return 0;
        //for each eleemnt of TD array, find if
        for (int i = 0; i < TDarray.size(); i++){
            if(TDarray.get(i).fieldName == name)
                return i;
        }
        //else if we haven't found it, we throw exception
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int sum = 0;
        for (int i = 0; i < TDarray.size(); i++){
            sum += (TDarray.get(i).fieldType).;
        }
        //TODO:: FINISH THIS FUNCTION
        return sum;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        //return null;
        ArrayList<Type> typearray = new ArrayList<Type>();
        ArrayList<String> stringarray = new ArrayList<String>();

        //for every field in desc 1, add the types and names to a array list
        for (int i = 0; i < td1.numFields(); i++){
            typearray.add(td1.getFieldType(i));
            stringarray.add(td1.getFieldName(i));
        }

        //do the same for desc2, td2's values will be at the end
        for (int i = 0; i < td2.numFields(); i++){
            typearray.add(td2.getFieldType(i));
            stringarray.add(td2.getFieldName(i));
        }

        //convert to arrays from array lists
        Type[] tempType = typearray.toArray(new Type[typearray.size()]);
        String[] tempString = stringarray.toArray(new String[stringarray.size()]);

        //use constructor to create new Tuple Desc
        TupleDesc result = new TupleDesc(tempType, tempString);
        return result;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        //we can only compare to string

    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String toReturn = "";
        for (int i = 0; i < TDarray.size(); i++){
            //converts item to string and appends to string
            toReturn += TDarray.get(i).toString() + ",";
        }
        return toReturn;
    }
}
