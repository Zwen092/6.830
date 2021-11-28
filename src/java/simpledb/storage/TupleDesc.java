package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TDItem tdItem = (TDItem) o;

            if (fieldType != tdItem.fieldType) return false;
            if (fieldName != null) return fieldName.equals(tdItem.fieldName);
            else {
                return tdItem.fieldName == null;
            }
        }

        @Override
        public int hashCode() {
            int result = fieldType.hashCode();
            result = 31 * result + fieldName.hashCode();
            return result;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private final TDItem[] tdItems;
    private int length;

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    //todo: Implement toString() in tupleDesc
    public TDItem[] getTdItems() {
        return tdItems;
    }
    public Iterator<TDItem> iterator() {
        // some code goes here
        return Arrays.asList(tdItems).iterator();
    }


    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        int len = typeAr.length;
        tdItems = new TDItem[len];
        for (int i = 0; i < len; i++) {
            tdItems[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
        length = len;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        int len = typeAr.length;
        tdItems = new TDItem[len];
        for (int i = 0; i < len; i++) {
            tdItems[i] = new TDItem(typeAr[i], null);
        }
        length = len;
    }

    public TupleDesc(TDItem[] tdItems) {
        this.tdItems = tdItems;
        length = this.tdItems.length;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tdItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        return tdItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        return tdItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < length; i++) {
            if (tdItems[i].fieldName != null && tdItems[i].fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int totalSize = 0;
        for (int i = 0; i < length; i++) {
            if (tdItems[i].fieldType == Type.INT_TYPE) {
                totalSize += Type.INT_TYPE.getLen();
            } else {
                totalSize += Type.STRING_TYPE.getLen();
            }
        }
        return totalSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int len1 = td1.length;
        int len2 = td2.length;
        TDItem[] tdItems = new TDItem[len1 + len2];
        for (int i = 0; i < td1.length; i++) {
            tdItems[i] = td1.tdItems[i];
        }
        for (int i = td1.length; i < td2.length + td1.length; i++) {
            tdItems[i] = td2.tdItems[i - td1.length];
        }
        TupleDesc res = new TupleDesc(tdItems);
        return res;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TupleDesc tupleDesc = (TupleDesc) o;
        if (this.length != tupleDesc.length) return false;
        for (int i = 0; i < length; i++) {
            if (!tdItems[i].equals(tupleDesc.tdItems[i])) return false;
        }
        return true;
    }

    //todo: Implement hashcode() in tupleDesc
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
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < this.numFields(); i++) {
            TDItem tdItem =  tdItems[i];
            stringBuilder.append("(")
                    .append(tdItem.fieldType.toString())
                    .append("[").append(i).append("]")
                    .append("(").append(tdItem.fieldName)
                    .append("[").append(i).append("])");
        }
        return stringBuilder.toString();
    }
}
