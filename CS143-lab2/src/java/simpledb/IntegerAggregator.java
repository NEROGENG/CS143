package simpledb;


import java.util.ArrayList;
import java.util.HashMap;
/**
* Knows how to compute some aggregate over a set of IntFields.
*/
public class IntegerAggregator implements Aggregator {
      private static final long serialVersionUID = 1L;
      int gbfield;
      Type gbfieldtype;
      int afield;
      Op what;
      TupleDesc tupleDesc;
      ArrayList<Tuple> list;
      HashMap<Field, ArrayList<Integer>> counts;
      /**
      * Aggregate constructor
      *
      * @param gbfield
      * the 0-based index of the group-by field in the tuple, or
      * NO_GROUPING if there is no grouping
      * @param gbfieldtype
      * the type of the group by field (e.g., Type.INT_TYPE), or null
      * if there is no grouping
      * @param afield
      * the 0-based index of the aggregate field in the tuple
      * @param what
      * the aggregation operator
      */
        public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

          // grouping {g, a}
          if (gbfield != Aggregator.NO_GROUPING) {
            Type[] typeArray = {gbfieldtype, Type.INT_TYPE};
            this.tupleDesc = new TupleDesc(typeArray);
          }

          //no grouping
          else {
            Type[] typeArray = {Type.INT_TYPE};
            this.tupleDesc = new TupleDesc(typeArray);
          }
            list = new ArrayList<Tuple>();
            counts = new HashMap<Field, ArrayList<Integer>>();
        }


        /**
        * Merge a new tuple into the aggregate, grouping as indicated in the
        * constructor
        *
        * @param tup
        * the Tuple containing an aggregate field and a group-by field
        */
        public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        //check for grouping
        if (gbfield == Aggregator.NO_GROUPING){
          //initialize for empty list
          if (list.size() == 0) {
              IntField f = (IntField) tup.getField(afield);
              ArrayList<Integer> values = new ArrayList<Integer>();
              values.add(f.getValue());
              counts.put(new IntField(Aggregator.NO_GROUPING), values);

              if (what == Op.COUNT) {
                  tup.setField(afield, new IntField(1));
              }
              list.add(tup);
              return;
          }

        // switch (what.toString()) {
        //     case "min": 
        if (what.toString() == "min"){
            System.out.println("min function:" + tup.toString());
              if (list.get(0).getField(afield).compare(Predicate.Op.GREATER_THAN, tup.getField(afield))) {
                  list.set(0, tup);
                  System.out.println("min function:assigned");
                }
              }
        if (what == MAX){
            if (list.get(0).getField(afield).compare(Predicate.Op.LESS_THAN, tup.getField(afield))){
              list.set(0, tup);
            }
        }
        if (what == SUM){
          //update sum, get the 0th value and then add the new tuple value
          IntField f1 = (IntField)(tup.getField(afield));
          IntField f2 = (IntField)(list.get(0).getField(afield));
          Field f = new IntField(f1.getValue() + f2.getValue());
          //calculate new sum, add to 0th field

          list.get(0).setField(afield, f);
        }
        if (what == COUNT){
          //update list.get(0)'s field with a new count of array
            ArrayList<Integer> intarr = counts.get(new IntField(Aggregator.NO_GROUPING));
            IntField input = (IntField)tup.getField(afield);
            intarr.add(input.getValue());
            //create new field based off new conut
            IntField f1 = new IntField(intarr.size());
            list.get(0).setField(afield,f1);



        }
        if (what == AVG){
          //calculate the average and update the 0th field
          //get every value of the whole list and caluclate sum
          int sum = 0;
          //convert from field to integer, but we have hash map
          ArrayList<Integer> intarr = counts.get(new IntField(Aggregator.NO_GROUPING));  

          
          for (int i = 0; i < intarr.size(); i++){
              sum += intarr.get(i);
          }
          //convert tuple into intfield, and calbulate sum
          IntField input = (IntField) tup.getField(afield);
          sum += input.getValue();
          //add tuple value
          intarr.add(input.getValue());
          int avg =  sum/(intarr.size());

          //now add to list and intarr
          IntField f3 = new IntField(avg);
          list.get(0).setField(afield, f3);


        }

              //break;
            // case MAX: 
            //   if (list.get(0).getField(afield).compare(Predicate.Op.LESS_THAN, tup.getField(afield))) 
            //   list.set(0, tup);
            //   break;
            // case SUM: 
            //   IntField f1 = (IntField)(list.get(0).getField(afield));
            //   IntField f2 =(IntField)(tup.getField(afield));
            //   Field f = new IntField(f1.getValue() + f2.getValue());
            //   list.get(0).setField(afield, f);
            //   break;
            // case AVG: 
            // ArrayList<Integer> values = counts.get(new IntField(Aggregator.NO_GROUPING));
            // IntField fTemp = (IntField) tup.getField(afield);
            // values.add(fTemp.getValue());
            // int sum = 0;
            // for (int j = 0; j < values.size(); j++){
            //   sum += values.get(j);
            // }
            // int calculation = sum/values.size();
            // IntField f3 = new IntField(calculation);
            // list.get(0).setField(afield, f3);
                       
            // break;
            // case COUNT: 
            // ArrayList<Integer> values1 = counts.get(new IntField(Aggregator.NO_GROUPING));
            // IntField fTemp1 = (IntField) tup.getField(afield);
            // values1.add(fTemp1.getValue());
            // IntField f4 = new IntField(values1.size());
            // list.get(0).setField(afield, f4);
            // break;
        //}
        }// end if not grouping
          else {

          if (list.size() == 0) {
                Field f1 = tup.getField(gbfield);
                IntField f2 = (IntField) tup.getField(afield);
                ArrayList<Integer> values = new ArrayList<Integer>();
                values.add(f2.getValue());
                counts.put(f1, values);
                if (what == Op.COUNT) {
                    tup.setField(afield, new IntField(1));
                }
                list.add(tup);
                return;
          }
          switch (what) {
          case MIN: 
                System.out.println("min function" +tup.toString());
                for (int i = 0; i < list.size(); i++){
                    if (list.get(i).getField(gbfield).equals(tup.getField(gbfield))){
                  boolean comp = list.get(i).getField(afield).compare(Predicate.Op.GREATER_THAN, tup.getField(afield));
                  if (comp) {
                    list.set(i, tup);
                  System.out.println("set");
                }
              }}
                  break;
          case MAX: 

                for (int i = 0; i < list.size(); i++){
                if (list.get(i).getField(gbfield).equals(tup.getField(gbfield))){
                boolean comp = list.get(i).getField(afield).compare(Predicate.Op.LESS_THAN, tup.getField(afield));
                if (comp) 
                list.set(i, tup);}
            }

                break;
          case SUM: sumGroupMerge(tup);
          break;
          case AVG: avgGroupMerge(tup);
          break;
          case COUNT: countGroupMerge(tup);
          break;
      }//end switch
      }//end else
}//end function


public void minGroupMerge(Tuple tup){
for (int i = 0; i < list.size(); i++){
if (list.get(i).getField(gbfield).equals(tup.getField(gbfield))){
boolean comp = list.get(i).getField(afield).compare(Predicate.Op.GREATER_THAN, tup.getField(afield));
if (comp) {
list.set(i, tup);
}
return;
}
}
list.add(tup);
}
public void maxGroupMerge(Tuple tup){
for (int i = 0; i < list.size(); i++){
if (list.get(i).getField(gbfield).equals(tup.getField(gbfield))){
boolean comp = list.get(i).getField(afield).compare(Predicate.Op.LESS_THAN, tup.getField(afield));
if (comp) {
list.set(i, tup);
}
return;
}
}
list.add(tup);
}
public void sumGroupMerge(Tuple tup){
for (int i = 0; i < list.size(); i++){
if (list.get(i).getField(gbfield).equals(tup.getField(gbfield))){
IntField f1 = (IntField)(list.get(i).getField(afield));
IntField f2 =(IntField)(tup.getField(afield));
Field f = new IntField(f1.getValue() + f2.getValue());
list.get(i).setField(afield, f);
return;
}
}
list.add(tup);
}
public void avgGroupMerge(Tuple tup){
for (int i = 0; i < list.size(); i++){
if (list.get(i).getField(gbfield).equals(tup.getField(gbfield))){
Field f1 = list.get(i).getField(gbfield);
ArrayList<Integer> values = counts.get(f1);
IntField fTemp = (IntField) tup.getField(afield);
values.add(fTemp.getValue());
int sum = 0;
for (int j = 0; j < values.size(); j++){
sum += values.get(j);
}
int calculation = sum/values.size();
IntField f2 = new IntField(calculation);
list.get(i).setField(afield, f2);
return;
}
}
list.add(tup);
Field f1 = tup.getField(gbfield);
IntField f2 = (IntField) tup.getField(afield);
ArrayList<Integer> values = new ArrayList<Integer>();
values.add(f2.getValue());
counts.put(f1, values);
}
          public void countGroupMerge(Tuple tup){

              for (int i = 0; i < list.size(); i++){

              if (list.get(i).getField(gbfield).equals(tup.getField(gbfield))){

                  Field f1 = list.get(i).getField(gbfield);
                  ArrayList<Integer> values = counts.get(f1);
                  IntField fTemp = (IntField) tup.getField(afield);
                  values.add(fTemp.getValue());
                  IntField f2 = new IntField(values.size());
                  list.get(i).setField(afield, f2);
                  return;
                  }
              }
                Field f1 = tup.getField(gbfield);
                IntField f2 = (IntField) tup.getField(afield);
                ArrayList<Integer> values = new ArrayList<Integer>();
                values.add(f2.getValue());
                counts.put(f1, values);
                tup.setField(afield, new IntField(1));
                list.add(tup);
          } 
          /**
          * Create a DbIterator over group aggregate results.
          *
          * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
          * if using group, or a single (aggregateVal) if no grouping. The
          * aggregateVal is determined by the type of aggregate specified in
          * the constructor.
          */
          public DbIterator iterator() {
              // some code goes here
                  return new TupleIterator(tupleDesc, list);
              }
}

    
