package com.nereus;

import net.agkn.hll.HLL;
import org.javatuples.Octet;
import org.javatuples.Pair;
import org.javatuples.Sextet;
import org.javatuples.Triplet;
import static com.nereus.HLLDB.*;

import java.util.*;

public class Builder {

    public static void joinNodes()
    {

        Iterator<String> left = HLLDB.NODES.iterator();
        ArrayList<String> tempList = new ArrayList<>();
        ArrayList<String> regrouped = new ArrayList<>();


        if(!left.hasNext())
        {
            System.out.println("No more elements");
            // joinNodes5(tempGraph);
            //return tempGraph;
        }

        while (left.hasNext()){
            Iterator<String> right = HLLDB.NODES.iterator();
            String lname = left.next();


            String superL = new String();
            /** if (leftNode.identifiers.iterator().hasNext())
             {
             superL = leftNode.identifiers.iterator().next();
             System.out.println("SuperL "+superL);
             }**/


            //System.out.println("***************LNAME"+lname);
            while (right.hasNext())
            {
                String rname = right.next();
                Boolean hasJoined = MetadataService.hasBeenJoined2(lname,rname);
                // System.out.println("Lname"+lname+"RName "+rname);

                if(lname!=rname) {
                    /**This part should contain 3 conditions **/


                    /** JOIN on supersets **/
                    Collection<Sextet> supersets = (Collection) SUPERSET.get(lname);
                    if(supersets!= null)
                    {

                        //maybe superset-subset relationship
                        if(ProcessorService.supersetContainsTable(lname,rname))

                        {

                            if(!hasJoined){

                                if(MetadataService.isSubset2(lname,rname)) {
                                    //TODO Perhaps make it posible to join on supersets
                                    //TODO In current POC it was never really an issue

                                }
                            }
                        }

                    }
                    /**JOIN on subsets **/

                    // Collection<Sextet> col = (Collection) subset.get(lname);
                    Collection<Sextet> col = ProcessorService.getJoiningTuple2(lname,rname);
                    //there is subset for that node
                    if (col != null ) {
                        Iterator<Sextet> iter = col.iterator();
                        while (iter.hasNext()) {

                            Sextet sextet = iter.next();

                            /**Subset join sextet **/
                            //   if(sextet.getValue0().equals(rname))
                            if(MetadataService.isSubset2(lname,rname))
                            {
                                //Triplet<String,String,Boolean> merged= joinTriplet.getOrDefault(rname,defaultTriplet);


                                // if(!merged.getValue2()) {
                                if(!hasJoined){
                                    //TODO has no ancestors here in the condition join only if there ar no other dependencies
                                    //if(merged.getValue0()!=lname& merged.getValue1()!=rname) {


                                    //subset.remove(leftNode.name);
                                    // subset.removeMapping(leftNodeName,sextet);
                                    //TODO join procedure
                                    SEPTET.put(lname, sextet);

                                    //TODONE get coresponding superset and put it in spetet

                                    //removeRelations(lname,rname);
                                    ProcessorService.removeRelations(sextet.getValue0().toString(),sextet.getValue3().toString());

                                    System.out.println("*************JOIN on superset -subset  relation ");
                                    System.out.println(sextet.toString());
                                    System.out.println("LeftNode " + lname);
                                    System.out.println("SEX " + sextet.getValue0());
                                    System.out.println("Sextet get values(3)"+sextet.getValue3());
                                    System.out.println("Right " + rname);
                                    System.out.println("JoinTriplet "+JOINTRIPLET.toString());
                                    Triplet<String, String, Boolean> joined = Triplet.with(lname, rname, false);
                                    Triplet<String, String, Boolean> joined2 = Triplet.with(rname, lname, false);
                                    JOINTRIPLET.put(lname,joined);
                                    JOINTRIPLET.put(rname,joined2);
                                    String newTableName = lname+"And"+rname;
                                    /**added this to get left and right key **/
                                    /**METADATA*********************************/
                                    //septet.put(newTableName,sextet);
                                    JOINS.put(newTableName,sextet);
                                    JOINSSTARTEND.put(lname,newTableName);
                                    JOINSSTARTEND.put(rname,newTableName);
                                    Pair<String,String> startEnd = Pair.with(lname,rname);
                                    JOINSSTARTEND2.put(newTableName,startEnd);
                                    //JOINORREGROUPTRIPLET.put(lname,joined);
                                    //JOINORREGROUPTRIPLET.put(rname,joined2);
                                    JOINORREGROUPTRIPLET.put(newTableName,joined);

                                    /******************************************/
                                    /**  if(!tempList.contains(newTableName)){
                                     tempList.add(newTableName);
                                     }**/

                                    if(!HLLDB.TEMPJOINS.contains(newTableName)){
                                        HLLDB.TEMPJOINS.add(newTableName);
                                    }
                                    //TODO copy properties to the new table
                                    ProcessorService.transferRelations(lname,rname,newTableName);
                                    //right table is the superset get identfier
                                    HLLDB.IDENTIFIEDBY.put(newTableName,sextet.getValue4().toString());
                                    //TODO: new stuff
                                    Set<String> tmpSet = new HashSet<>();
                                    tmpSet.add(sextet.getValue4().toString());
                                    IDENTIFIEDBY2.put(newTableName,tmpSet);

                                }
                            }

                            //  }


                            //System.out.println(septet.get(lname)); //subset.

                        }
                        //opy all the stuff and create new node


                    }


                    //if (MetadataService.checkMatchProperSubset(lname,rname))
                    //testing with Dictionary based on BK-FK realtionship. Before it was based on MaxCardinality
                    if (MetadataService.checkMatchProperSubsetWithDictionary(lname,rname))
                    {
                        System.out.println("checkingMatchProperSubset "+ lname +" rname "+rname);
                    }

                }
            }




        }
        System.out.println("Adding nodes ");
        /**   if(!nodes.containsAll(tempList)){
         System.out.println("Adding newly joined nodes "+tempList.toString());
         nodes.addAll(tempList);
         }**/

        if(!HLLDB.NODES.containsAll(HLLDB.TEMPJOINS)){
            System.out.println("Adding newly joined nodes "+HLLDB.TEMPJOINS.toString());
            HLLDB.NODES.addAll(HLLDB.TEMPJOINS);
            HLLDB.TEMPJOINS.clear();
        }

      /*  if(!nodes.containsAll(regrouped)){
            System.out.println("Adding regrouped nodes "+regrouped.toString());
            nodes.addAll(regrouped);
        }*/

        if(!HLLDB.NODES.containsAll(HLLDB.TEMPREGROUPED)){

            System.out.println("Adding regrouped nodes "+HLLDB.TEMPREGROUPED.toString());
            HLLDB.NODES.addAll(HLLDB.TEMPREGROUPED);
            HLLDB.TEMPREGROUPED.clear();
        }


    }


    public static void fit ( HashMap<String,HashMap<Set<String>,HLL>> left, HashMap<String,HashMap<Set<String>,HLL>> right,Set<String> setAName,Set<String> setBName ) throws CloneNotSupportedException
    {


        String leftNameDepracated = setAName.toString().replace("[","").replace("]","").toString();
        String rightNameDepracated = setBName.toString().replace("[","").replace("]","").toString();

        Set<String> policy_pk;
        String identifier = new String();
        Integer [] setACardinality = new Integer[1];
        Integer [] setBCardinality = new Integer[1];
        HLL policyHLL = new HLL(14, 5);
        HLL hllA1 = new HLL(14,5);


        Set<Set<String>> leftTagSets = left.get(leftNameDepracated).keySet();
        Set<Set<String>> rightTagSets = right.get(rightNameDepracated).keySet();



        // System.out.println("--------------------");
        // System.out.println("Set A" +polTagSets.toString());
        //  System.out.println("Set B"+customerTagSets.toString());
        //int leftRows = SetInfo.getCardinality(leftNameDepracated);
        //int rightRows = SetInfo.getCardinality(rightNameDepracated);
        int leftRows = SetInfo.getCardinalityA(leftNameDepracated);
        int rightRows = SetInfo.getCardinalityA(rightNameDepracated);
        // System.out.println("Number of leftRows for SetA "+leftRows);


        //Set<String> subsets = new HashSet<>();
        //Set<String> identifiers = new HashSet<>();
        //Set<String> subidentifiers = new HashSet<>();

        // System.out.println("--------------------");

        //Set <String> temp = new HashSet<>();
        for (Set<String> set : leftTagSets) {
            policy_pk = set;
            //  System.out.println("*****************************************");
            //System.out.println("PolKeys"+set.toString() + polCardinalities.get(policy_pk).cardinality());
            // System.out.println("SetX "+set);
            // System.out.println("SetX2 "+set.toString().equals("[customertime, policy]"));
            // if(left.get(leftNameDepracated).get(policy_pk)!=null) {
            policyHLL = left.get(leftNameDepracated).get(policy_pk);
            long temp1 = policyHLL.cardinality();
            //  System.out.println("Cardinality policyHLL "+policyHLL.cardinality());
            //  System.out.println("###########################################");
            if(set.size()==1 && !set.isEmpty())
            {
                //Used for BK discovery for now only on single columns
               // System.out.println("********************************************");

                int cardinality = (int) policyHLL.cardinality();
                String column = set.toString().replace("[","").replace("]","").toString();
                 HashMap<Set<String>,Integer> tempMap = SETCARDINALITY.get(leftNameDepracated);

                 if (tempMap!=null) {
                     tempMap.put(set,cardinality);
                     SETCARDINALITY.put(leftNameDepracated,tempMap);
                 }
               // System.out.println(leftNameDepracated);
               // System.out.println(cardinality+"  "+column);
               //   System.out.println("Is business key "+MetadataService.isBusinessKey2(leftNameDepracated,set) );
               // System.out.println("********************************************");
            }

            for (Set<String> t : rightTagSets) {

                if (set.size()==t.size()) {
                    hllA1 = policyHLL.clone();
                    // System.out.println("HLLA1Cardinality "+hllA1.cardinality());
                    // System.out.println("CustKeys"+t.toString());

                    /**Get the second dataset to comapare with A*/
                    HLL hllB = right.get(rightNameDepracated).get(t);
                    long leftCaridinality = hllA1.cardinality();
                    long rightCardinality = hllB.cardinality();
                    // hllA1.clear();
                    hllA1.union(hllB);
                    //hllA1.cardinality()
                    //Metadata used for BK deduction


                   /**  System.out.println("Set A " + set);
                     System.out.println("Set A size "+set.size());
                     System.out.println("Set A cardinality " + leftCaridinality);
                     System.out.println("Set B " + t);
                     System.out.println("Set B size " + t.size());
                     System.out.println("Set B caridinality " + rightCardinality);
                     System.out.println("Union" + hllA1.cardinality());**/

                    long aU = hllA1.cardinality();
                    long inter = (leftCaridinality + rightCardinality) - aU;
                    //TODO approximately greater procedure
                    //TODO for now let's use 50-100 1-2% error rate
                    Octet<String,Set<String>,Long,String,Set<String>,Long,Long,Long> unionInter = Octet.with(leftNameDepracated,set,leftCaridinality,rightNameDepracated,t,rightCardinality,aU,inter);
                    UNIONINTER.put(leftNameDepracated,unionInter);
                  // if (inter > 0) { //ABC AVRO3
                   // if (inter > 36) { //AVRO4
                   // if (inter >0) { //AVRO3
                  // if(inter>520) { //DEF
                    if(inter>21) { //AVRO 5
                        // if (inter>2500){

                        //System.out.println("*****************************************");

                        //System.out.println("Cardinality policyHLL "+policyHLL.cardinality());
                        /**  System.out.println("-------------------");
                         System.out.println("NodeName A " + leftNameDepracated);
                         System.out.println("NodeNamed B " + rightNameDepracated);
                         System.out.println("For key in  A");
                         //System.out.println("PolKeys"+set.toString() + polCardinalities.get(policy_pk).cardinality());
                         System.out.println(set.toString());


                         // System.out.println("HLLA1Cardinality "+hllA1.cardinality());
                         System.out.println("We find key in  B");
                         System.out.println(t.toString());

                         System.out.println("Set A " + set.toString() + " cardinality " + leftCaridinality);
                         System.out.println("Set B " + t.toString() + " caridinality " + rightCardinality);
                         System.out.println("Union" + hllA1.cardinality());
                         System.out.println("Intersection " + inter);**/

                        if (leftCaridinality == inter || rightCardinality == inter) {

                            //subsets.add(set.toString());
                            //subsets.add(t.toString());
                            //setsTable.put(set.toString(),t.toString()); //new

                            //if cardinality not equals left rows but we know there is intersection needs group by put it into proper subset.
                            // if (leftCaridinality != leftRows) {
                            if(!MetadataService.approximatelyEqual(leftCaridinality,leftRows,2)){
                                Sextet<String, Set<String>, Long, String, Set<String>, Long> tempSextet =
                                        Sextet.with(leftNameDepracated, set, leftCaridinality, rightNameDepracated, t, rightCardinality);
                                Sextet<String, Set<String>, Long, String, Set<String>, Long> rightSextet =
                                        Sextet.with(rightNameDepracated, t, rightCardinality, leftNameDepracated, set, leftCaridinality);
                                if(!leftNameDepracated.isEmpty()) {
                                    //Get rid of empty sets [] 4/26/2018 getValue5.isEmpty not possible
                                    if(!set.isEmpty() && !t.isEmpty() ){


                                    PROPERSUBSET.put(leftNameDepracated, tempSextet);
                                    }
                                }
                                /**add values on the other side of the relationship**/
                                /**if(!properSubset.containsValue(rightSextet)){
                                 properSubset.put(rightName,rightSextet);}**/
                                System.out.println("-------------------");
                                System.out.println("NodeName A " + leftNameDepracated);
                                System.out.println("NodeNamed B " + rightNameDepracated);
                                System.out.println(set.toString() + "isProper Subset " + t.toString());
                                /*************************************************************************/

                                //System.out.println("NodeName A " + leftNameDepracated);
                                //System.out.println("NodeNamed B " + rightNameDepracated);
                                /** System.out.println("For key in  A");
                                 //System.out.println("PolKeys"+set.toString() + polCardinalities.get(policy_pk).cardinality());
                                 System.out.println(set.toString());


                                 // System.out.println("HLLA1Cardinality "+hllA1.cardinality());
                                 System.out.println("We find key in  B");
                                 System.out.println(t.toString());

                                 System.out.println("Set A " + set.toString() + " cardinality " + leftCaridinality);
                                 System.out.println("Set B " + t.toString() + " caridinality " + rightCardinality);
                                 System.out.println("Union" + hllA1.cardinality());
                                 System.out.println("Intersection " + inter);**/
                                /************************************************************/

                            }
                            System.out.println(set.toString() + "Proper Subset " + t.toString());

                            //  if (inter == rightRows && leftCaridinality==leftRows ) {
                            if(MetadataService.approximatelyEqual(inter,rightRows,2)&&MetadataService.approximatelyEqual(leftCaridinality,leftRows,2)){
                                // if (inter == rightRows  ) {
                                System.out.println("Nodename " + rightNameDepracated + " is subset of " + leftNameDepracated + " which is superset");
                                Sextet<String, Set<String>, Long, String, Set<String>, Long> tempSextet =
                                        Sextet.with(leftNameDepracated, set, leftCaridinality, rightNameDepracated, t, rightCardinality);
                                //superset.put(leftName,tempSextet);
                                Sextet<String, Set<String>, Long, String, Set<String>, Long> tempSextet2 =
                                        Sextet.with(rightNameDepracated, t, rightCardinality, leftNameDepracated, set, leftCaridinality);
                                if(!leftNameDepracated.isEmpty()) {
                                    if(!set.isEmpty() && !t.isEmpty()){
                                    SUBSET.put(rightNameDepracated, tempSextet2);
                                    }
                                }// change from tempSextet2

                            }

                            // if (leftCaridinality == leftRows) {
                            if(MetadataService.approximatelyEqual(leftCaridinality,leftRows,2)){
                                // System.out.println("Number of leftRows " + leftRows);
                                //System.out.println("This is primary key");

                                if(!leftNameDepracated.isEmpty()) {
                                    IDENTIFIEDBY.put(leftNameDepracated, set);
                                }
                                //TODO new stuff

                                if(!leftNameDepracated.isEmpty()) {
                                    if(IDENTIFIEDBY2.get(leftNameDepracated)!=null){
                                        // Set<String> set1= new HashSet<>((ArrayList<String>)HLLDB.identifiedBy.get(leftNameDepracated));
                                        Set<String> set1= (Set<String>) IDENTIFIEDBY2.get(leftNameDepracated);
                                        // ArrayList<Set<String>> listOfSets = (ArrayList<Set<String>>)HLLDB.identifiedBy2.get(leftNameDepracated);
                                        // int set1Size = listOfSets.get(0).size();

                                        //MetadataService.isGreaterThan(set1,set);
                                        if(set1.size()>set.size()){
                                            IDENTIFIEDBY2.put(leftNameDepracated, set);
                                        }


                                    } else
                                    { IDENTIFIEDBY2.put(leftNameDepracated, set);}
                                }
                                if ( leftCaridinality >= rightCardinality ) {
                                    // if(true) {
                                    Sextet<String, Set<String>, Long, String, Set<String>, Long> tempSextet =
                                            Sextet.with(leftNameDepracated, set, leftCaridinality, rightNameDepracated, t, rightCardinality);
                                    if(!leftNameDepracated.isEmpty()) {
                                        SUPERSET.put(leftNameDepracated, tempSextet);
                                    }
                                    //identifiers.add(set.toString());
                                    identifier = set.toString();
                                    // temp.add(t.toString());

                                    // System.out.println(set.toString());

                                }

                               // else if (leftCaridinality < rightCardinality && rightCardinality<rightRows ) {
                               else if (leftCaridinality < rightCardinality && !MetadataService.approximatelyEqual(rightCardinality,rightRows,2) ) {
                                    //setsTable.put(set.toString(),t.toString());
                                    //TODO add identifiers for the subsets???

                                    //Superset to properSubset relation
                                    //rightCardianlity is less then rightRows so it is a foreign key
                                    Sextet<String, Set<String>, Long, String, Set<String>, Long> tempSextet =
                                            Sextet.with(leftNameDepracated, set, leftCaridinality, rightNameDepracated, t, rightCardinality);
                                    if(!leftNameDepracated.isEmpty()) {
                                        SUPERSET.put(leftNameDepracated, tempSextet);
                                    }
                                    //subidentifiers.add(set.toString());
                                    //subidentifiers.add(t.toString());
                                    //setsTable.put(set.toString(),t.toString());  //new
                                }
                            }
                        } else {

                            /*** inter is greater than zero so there is some relationship, basically add everything that is somehow related**/
                            Sextet<String, Set<String>, Long, String, Set<String>, Long> tempSextet3 =
                                    Sextet.with(leftNameDepracated, set, leftCaridinality, rightNameDepracated, t, rightCardinality);
                            if(!leftNameDepracated.isEmpty()) {
                                OVERLAP.put(leftNameDepracated, tempSextet3);
                            }
                            //  System.out.println("Overlaps ******");

                            // System.out.println("A " + leftNameDepracated + " " + set.toString());
                            //  System.out.println("B " + rightNameDepracated + " " + t.toString());
                            //subsets.add(set.toString());
                            //subsets.add(t.toString());
                            //setsTable.put(set.toString(),t.toString()); //new

                        }


                    } //

                }

            } // inner loop
            //  } //if policy_pk not null



            //   }
            //System.out.println("*******************************************");

        } //outer loop

        //System.out.println("#########################################");

        // GraphNode graphNode = new GraphNode(leftName,identifiers,subsets,subidentifiers);
        // graphNode.cardinality=rows;


        //   graph.add(graphNode);
        //graphNode.print();



    }




    /**  public static void findCommonKeys3 ( HashMap<String,HashMap<Set<String>,HLL>> left, HashMap<String,HashMap<Set<String>,HLL>> right,Set<String> setAName,Set<String> setBName ) throws CloneNotSupportedException
     {


     String leftNameDepracated = setAName.toString().replace("[","").replace("]","").toString();
     String rightNameDepracated = setBName.toString().replace("[","").replace("]","").toString();

     Set<String> policy_pk;
     String identifier = new String();
     Integer [] setACardinality = new Integer[1];
     Integer [] setBCardinality = new Integer[1];
     HLL policyHLL = new HLL(14, 5);
     HLL hllA1 = new HLL(14,5);


     Set<Set<String>> leftTagSets = left.get(leftNameDepracated).keySet();
     Set<Set<String>> rightTagSets = right.get(rightNameDepracated).keySet();

     // System.out.println("--------------------");
     // System.out.println("Set A" +polTagSets.toString());
     //  System.out.println("Set B"+customerTagSets.toString());
     int leftRows = SetInfo.getCardinality(leftNameDepracated);
     int rightRows = SetInfo.getCardinality(rightNameDepracated);
     // System.out.println("Number of leftRows for SetA "+leftRows);


     //Set<String> subsets = new HashSet<>();
     //Set<String> identifiers = new HashSet<>();
     //Set<String> subidentifiers = new HashSet<>();

     // System.out.println("--------------------");

     //Set <String> temp = new HashSet<>();
     for (Set<String> set : leftTagSets) {
     policy_pk = set;
     //  System.out.println("*****************************************");
     //System.out.println("PolKeys"+set.toString() + polCardinalities.get(policy_pk).cardinality());
     System.out.println("SetX "+set);
     System.out.println("SetX2 "+set.toString().equals("[customertime, policy]"));
     // if(left.get(leftNameDepracated).get(policy_pk)!=null) {
     policyHLL = left.get(leftNameDepracated).get(policy_pk);
     //  System.out.println("Cardinality policyHLL "+policyHLL.cardinality());
     //  System.out.println("###########################################");

     for (Set<String> t : rightTagSets) {

     if (set.size()==t.size()) {
     hllA1 = policyHLL.clone();
     // System.out.println("HLLA1Cardinality "+hllA1.cardinality());
     // System.out.println("CustKeys"+t.toString());


     HLL hllB = right.get(rightNameDepracated).get(t);
     long leftCaridinality = hllA1.cardinality();
     long rightCardinality = hllB.cardinality();
     // hllA1.clear();
     hllA1.union(hllB);

     System.out.println("Set A " + set);
     System.out.println("Set A size "+set.size());
     System.out.println("Set A cardinality " + leftCaridinality);
     System.out.println("Set B " + t);
     System.out.println("Set B size " + t.size());
     System.out.println("Set B caridinality " + rightCardinality);
     System.out.println("Union" + hllA1.cardinality());

     long aU = hllA1.cardinality();
     long inter = (leftCaridinality + rightCardinality) - aU;
     //inter = inter - 10000;
     if (inter > 1) {

     //System.out.println("*****************************************");

     //System.out.println("Cardinality policyHLL "+policyHLL.cardinality());
     System.out.println("-------------------");
     System.out.println("NodeName A " + leftNameDepracated);
     System.out.println("NodeNamed B " + rightNameDepracated);
     System.out.println("For key in  A");
     //System.out.println("PolKeys"+set.toString() + polCardinalities.get(policy_pk).cardinality());
     System.out.println(set.toString());


     // System.out.println("HLLA1Cardinality "+hllA1.cardinality());
     System.out.println("We find key in  B");
     System.out.println(t.toString());

     System.out.println("Set A " + set.toString() + " cardinality " + leftCaridinality);
     System.out.println("Set B " + t.toString() + " caridinality " + rightCardinality);
     System.out.println("Union" + hllA1.cardinality());
     System.out.println("Intersection " + inter);

     if (leftCaridinality == inter || rightCardinality == inter) {

     //subsets.add(set.toString());
     //subsets.add(t.toString());
     //setsTable.put(set.toString(),t.toString()); //new

     //if cardinality not equals left rows but we know there is intersection needs group by put it into proper subset.
     // if (leftCaridinality != leftRows) {
     if(!MetadataService.approximatelyEqual(leftCaridinality,leftRows,1)){
     Sextet<String, Set<String>, Long, String, Set<String>, Long> tempSextet =
     Sextet.with(leftNameDepracated, set, leftCaridinality, rightNameDepracated, t, rightCardinality);
     Sextet<String, Set<String>, Long, String, Set<String>, Long> rightSextet =
     Sextet.with(rightNameDepracated, t, rightCardinality, leftNameDepracated, set, leftCaridinality);
     if(!leftNameDepracated.isEmpty()) {
     PROPERSUBSET.put(leftNameDepracated, tempSextet);
     }
     System.out.println(set.toString() + "isProper Subset " + t.toString());
     }
     System.out.println(set.toString() + "Proper Subset " + t.toString());

     //  if (inter == rightRows && leftCaridinality==leftRows ) {
     if(MetadataService.approximatelyEqual(inter,rightRows,1)&&MetadataService.approximatelyEqual(leftCaridinality,leftRows,1)){
     // if (inter == rightRows  ) {
     System.out.println("Nodename " + rightNameDepracated + " is subset of " + leftNameDepracated + " which is superset");
     Sextet<String, Set<String>, Long, String, Set<String>, Long> tempSextet =
     Sextet.with(leftNameDepracated, set, leftCaridinality, rightNameDepracated, t, rightCardinality);
     //superset.put(leftName,tempSextet);
     Sextet<String, Set<String>, Long, String, Set<String>, Long> tempSextet2 =
     Sextet.with(rightNameDepracated, t, rightCardinality, leftNameDepracated, set, leftCaridinality);
     if(!leftNameDepracated.isEmpty()) {
     SUBSET.put(rightNameDepracated, tempSextet2);
     }// change from tempSextet2

     }

     // if (leftCaridinality == leftRows) {
     if(MetadataService.approximatelyEqual(leftCaridinality,leftRows,1)){
     // System.out.println("Number of leftRows " + leftRows);
     //System.out.println("This is primary key");

     if(!leftNameDepracated.isEmpty()) {
     if(IDENTIFIEDBY.get(leftNameDepracated)!=null){
     Set<String> set1= (Set<String>) IDENTIFIEDBY.get(leftNameDepracated);
     if(set1.size()>set.size()){
     IDENTIFIEDBY.put(leftNameDepracated, set);
     }


     } else
     { IDENTIFIEDBY.put(leftNameDepracated, set);}
     }
     if ( leftCaridinality >= rightCardinality ) {
     // if(true) {
     Sextet<String, Set<String>, Long, String, Set<String>, Long> tempSextet =
     Sextet.with(leftNameDepracated, set, leftCaridinality, rightNameDepracated, t, rightCardinality);
     if(!leftNameDepracated.isEmpty()) {
     SUPERSET.put(leftNameDepracated, tempSextet);
     }
     //identifiers.add(set.toString());
     identifier = set.toString();
     // temp.add(t.toString());

     // System.out.println(set.toString());
     } else if (leftCaridinality < rightCardinality && rightCardinality<rightRows ) {
     //setsTable.put(set.toString(),t.toString());
     //TODO add identifiers for the subsets???

     //Superset to properSubset relation
     Sextet<String, Set<String>, Long, String, Set<String>, Long> tempSextet =
     Sextet.with(leftNameDepracated, set, leftCaridinality, rightNameDepracated, t, rightCardinality);
     if(!leftNameDepracated.isEmpty()) {
     SUPERSET.put(leftNameDepracated, tempSextet);
     }
     //subidentifiers.add(set.toString());
     //subidentifiers.add(t.toString());
     //setsTable.put(set.toString(),t.toString());  //new
     }
     }
     } else {


     Sextet<String, Set<String>, Long, String, Set<String>, Long> tempSextet3 =
     Sextet.with(leftNameDepracated, set, leftCaridinality, rightNameDepracated, t, rightCardinality);
     if(!leftNameDepracated.isEmpty()) {
     OVERLAP.put(leftNameDepracated, tempSextet3);
     }
     System.out.println("Overlaps ******");

     System.out.println("A " + leftNameDepracated + " " + set.toString());
     System.out.println("B " + rightNameDepracated + " " + t.toString());
     //subsets.add(set.toString());
     //subsets.add(t.toString());
     //setsTable.put(set.toString(),t.toString()); //new

     }


     } //

     }

     } // inner loop
     //  } //if policy_pk not null



     //   }
     //System.out.println("*******************************************");

     } //outer loop

     //System.out.println("#########################################");

     // GraphNode graphNode = new GraphNode(leftName,identifiers,subsets,subidentifiers);
     // graphNode.cardinality=rows;


     //   graph.add(graphNode);
     //graphNode.print();



     }**/



}
