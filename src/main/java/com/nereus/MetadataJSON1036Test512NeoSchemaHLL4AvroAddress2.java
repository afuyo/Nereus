package com.nereus;


import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.nereus.AvroUtils.PopulateSampledData;
import com.nereus.Utils.NeoUtils;
import com.nereus.Utils.Output;
import net.agkn.hll.HLL;
import org.javatuples.Sextet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import static com.nereus.AvroUtils.Constants.*;
import static com.nereus.HLLDB.*;
//import java.lang.reflect.Array;

/**Adding second iteration **/

public class MetadataJSON1036Test512NeoSchemaHLL4AvroAddress2 {

 /** //  static ArrayList<String> nodes = HLLDB.nodes;
    //static ArrayList<String> streamnodes = HLLDB.streamnodes;
   // static ArrayList<String> tablenodes = HLLDB.tablenodes;
    static ArrayList<String> tempRegrouped = HLLDB.tempRegrouped;
    static ArrayList<String> tempJoins = HLLDB.tempJoins;
    //public static MultiMap<String,String> identifiedBy = HLLDB.identifiedBy;
    public static MultiMap<String,Sextet> superset = HLLDB.superset;
    public static MultiMap<String,Sextet> subset = HLLDB.subset;
    public static MultiMap<String,Sextet<String,Set<String>,Long,String,Set<String>,Long>> properSubset = HLLDB.properSubset;
    public static MultiMap<String,Sextet> overlap = HLLDB.overlap;
    public static MultiMap<String,Sextet> septet = HLLDB.septet;

    public static HashMap<String,Triplet> joinTriplet = HLLDB.joinTriplet;

    /**Graph Metadata ********************************************/
   /** public static MultiMap<String,Sextet> joins = HLLDB.JOINS;
    public static HashMap<String,String> joinsStartEnd = HLLDB.joinsStartEnd;
    public static HashMap<String,String> transformsStartEnd = HLLDB.transformsStartEnd;**/

    /**********************************************************************/

    /************************************************************************/
//"STATPEJ.POC_CLAIM","STATPEJ.POC_CLAIMPAYMENT","STATPEJ.POC_CUSTOMER","STATPEJ.POC_POLICY"
    public static String customerTopic ="STATPEJ.POC_CUSTOMER2";
    static String policyTopic ="STATPEJ.POC_POLICY2";
    static String claimTopic  = "STATPEJ.POC_CLAIM2";
    static String paymentTopic = "STATPEJ.POC_CLAIMPAYMENT2";
    static String addressTopic = "STATPEJ.POC_ADDRESS2";

    //public static Triplet<String,String,Boolean> defaultTriplet = Triplet.with("Missing","Missing",false);

    static public class SetInfo {
        static Integer customerLines = 0;//493;
        static Integer policyLines = 0;// 597;
        static Integer claimLines = 0;//448;
        static Integer paymentLines = 0;// 395;
        static Integer addressLines = 0;

        static int getCardinality2(String dataSetName)
        {

            switch(dataSetName){
                case CUSTOMER_TOPIC: return customerLines;

                case POLICY_TOPIC: return policyLines;

                case CLAIM_TOPIC: return claimLines;

                case PAYMENT_TOPIC: return paymentLines;

                case ADDRESS_TOPIC: return addressLines;

                default: return 0;

            }

        }
    }


    public static void findCommonKeys3 ( HashMap<String,HashMap<Set<String>,HLL>> left, HashMap<String,HashMap<Set<String>,HLL>> right,Set<String> setAName,Set<String> setBName ) throws CloneNotSupportedException
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
        int leftRows = SetInfo.getCardinality2(leftNameDepracated);
        int rightRows = SetInfo.getCardinality2(rightNameDepracated);
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
                //  System.out.println("Cardinality policyHLL "+policyHLL.cardinality());
                //  System.out.println("###########################################");

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

                   /** System.out.println("Set A " + set);
                    System.out.println("Set A size "+set.size());
                     System.out.println("Set A cardinality " + leftCaridinality);
                     System.out.println("Set B " + t);
                    System.out.println("Set B size " + t.size());
                     System.out.println("Set B caridinality " + rightCardinality);
                     System.out.println("Union" + hllA1.cardinality());**/

                    long aU = hllA1.cardinality();
                    long inter = (leftCaridinality + rightCardinality) - aU;

                    if (inter > 1) {

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
                                     PROPERSUBSET.put(leftNameDepracated, tempSextet);
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
                                   SUBSET.put(rightNameDepracated, tempSextet2);
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

   public static void generateGraph()
   {

       int next= NODES.size();
       int start= 0;
       int j= 1;

       while(start<next)
       {
           start=next;
           //joinNodes55();
           Builder.joinNodes();
           String s = "Metadata "+j+" for ";

           if(true) {
               NODES.forEach((e) -> {
                   System.out.println("'''''''''''***************** " + s + e);
                   Output.printMetaData(e);


               });
           }
           j++;
           next= NODES.size();
       }

   }

    public static void main(String[] arg) throws IOException, CloneNotSupportedException {
      //  System.out.println("Hello");
        //System.out.println(SetInfo.customerLines);

        HashFunction hashFunction = Hashing.murmur3_128();
        ArrayList<GraphNode> graph = new ArrayList<>();
        HashMap<String,String> setsTable = new HashMap();




        //SetInfo.paymentLines=i;

      //  HashMap<Set<String>,HLL> custCardinalities= new HashMap<>();
       // HashMap<Set<String>,HLL> polCardinalities  = new HashMap<>();
       // HashMap<Set<String>,HLL> claimCardinalities = new HashMap<>();
     //   HashMap<Set<String>,HLL> payCardinalities = new HashMap<>();
        //HashMap<Set<String>,HLL> addressCardinalities = new HashMap<>();


        HashMap<String,HashMap<Set<String>,HLL>> custCardinalities2= new HashMap<>();
        HashMap<String,HashMap<Set<String>,HLL>> polCardinalities2= new HashMap<>();
        HashMap<String,HashMap<Set<String>,HLL>> claimCardinalities2= new HashMap<>();
        HashMap<String,HashMap<Set<String>,HLL>> payCardinalities2= new HashMap<>();
        HashMap<String,HashMap<Set<String>,HLL>> addressCardinalities2= new HashMap<>();



        custCardinalities2.put(customerTopic,new HashMap<>());
        polCardinalities2.put(policyTopic,new HashMap<>());
        claimCardinalities2.put(claimTopic,new HashMap<>());
        payCardinalities2.put(paymentTopic,new HashMap());
        addressCardinalities2.put(addressTopic,new HashMap<>());



               //Collection of HLLs


        ArrayList <HashMap<String,HashMap<Set<String>,HLL>>> hllCollection = new ArrayList<>();
        hllCollection.add(custCardinalities2);
        hllCollection.add(polCardinalities2);
        hllCollection.add(claimCardinalities2);
        hllCollection.add(payCardinalities2);
        hllCollection.add(addressCardinalities2);

        //findCommonKeys(custCardinalities,polCardinalities);
        // findCommonKeys(claimCardinalities,payCardinalities);
        //findCommonKeys(polCardinalities,claimCardinalities);
        HashMap<String,Object> hashMap= PopulateSampledData.populateHLLObjects("STATPEJ.POC_CLAIM2,STATPEJ.POC_CLAIMPAYMENT2,STATPEJ.POC_CUSTOMER2,STATPEJ.POC_POLICY2,STATPEJ.POC_ADDRESS2", KAFKA_GROUP_ID,50000);
        // Schema schema= SchemaGeneratorUtils.generateJoinedSchema("Customer","Customer","Policy","Policy");
        // System.out.println("Schema "+schema.toString());
        @SuppressWarnings("unchecked")
        HashMap<String,String> schemas = (HashMap<String,String>)hashMap.get("schemaCollection");
         @SuppressWarnings("unchecked")
         ArrayList<HashMap<String, HashMap<Set<String>, HLL>>> hllCollection2 = (ArrayList<HashMap<String, HashMap<Set<String>, HLL>>> ) hashMap.get("hllCollection");

         hllCollection2.forEach((a) -> {

         Set<String> setAName = a.keySet();
         System.out.println("KeySet2 " + a.values().toString());
         });
        @SuppressWarnings("unchecked")
        HashMap<String, Long> dataCountMap =
                (HashMap<String, Long> )hashMap.get("dataCountCollection");
        //"STATPEJ.POC_CLAIM","STATPEJ.POC_CLAIMPAYMENT","STATPEJ.POC_CUSTOMER","STATPEJ.POC_POLICY"

        SetInfo.claimLines=dataCountMap.get(claimTopic).intValue();
        SetInfo.customerLines=dataCountMap.get(customerTopic).intValue();
        SetInfo.policyLines=dataCountMap.get(policyTopic).intValue();
        SetInfo.paymentLines=dataCountMap.get(paymentTopic).intValue();
        SetInfo.addressLines=dataCountMap.get(addressTopic).intValue();

        System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
        System.out.println(SetInfo.customerLines+" Customer Lines");
        System.out.println(SetInfo.policyLines+" Policy Lines");
        System.out.println(SetInfo.claimLines+" Claim Lines");
        System.out.println(SetInfo.paymentLines+" Payment lines");
        System.out.println(SetInfo.addressLines+" Address lines");

        hllCollection2.forEach((a) -> {

            Set<String> setAName = a.keySet();
            System.out.println("KeySet "+a.values().toString());
            hllCollection2.forEach((b)->{
                if (b.equals(a)){

                    //a.keySet()
                    //System.out.println("Equals");
                    //System.out.println(a.keySet());
                    //System.out.println(b.keySet());
                }
                else {
                    Set<String> setBName = b.keySet();
                    //System.out.println("Not Equals");
                    // System.out.println(a.keySet());
                    //System.out.println(b.keySet());
                    try {
                        // findCommonKeys2(a, b,setsTable);
                        findCommonKeys3(a, b,setAName,setBName);

                    } //catch( CloneNotSupportedException e)
                    catch(Exception e)
                    {
                        e.printStackTrace();
                    }

                }


            });
        });

        generateGraph();


        System.out.println("JoinTrippelt");
        JOINTRIPLET.forEach((k,v)->{
            System.out.println(k.toString());
            System.out.println(v.toString());
            //System.out.println(k.contains("Payment1"));
            //System.out.println(v.contains("Payment1"));
            // System.out.println(joinTriplet.keySet().contains("Payment1"));
        });



        System.out.println("Septet");



        SEPTET.forEach((k,v)->{
            System.out.println(k.toString()+" "+v.toString());

        });

        NeoUtils.writeToCSV();
        //NeoUtils.writeToNeo(schemas);
        //NeoUtils.writeToNeo2();
        NeoUtils.writeToNeo3(schemas);

       /** System.out.println("TempRegrouped "+(tempRegrouped.hashCode()));
        System.out.println("TempRegrouped "+(tempRegrouped.toString()));
        System.out.println("Idenfied by "+(IDENTIFIEDBY.hashCode()));
        System.out.println("Idenfied by "+(IDENTIFIEDBY.toString()));
        System.out.println("Septetst are equal "+(septet.hashCode()==TestClass.septet));
        System.out.println("SUPERSETs are equal "+(SUPERSET.hashCode()==TestClass.superset));
        System.out.println("Subsets are equal "+(subset.hashCode()==TestClass.subset));
        System.out.println("Overlap is equal "+(overlap.hashCode()==TestClass.overlap));
        System.out.println("JointTriplet is equal "+(joinTriplet.hashCode()==TestClass.jointriplet));
        System.out.println("Identified by "+(IDENTIFIEDBY.hashCode()==TestClass.identifiedBy));**/


        //  System.out.println("Get joining tuple for Customer & Claim"+getJoiningTuple("Customer","Claim").toString());
        //System.out.println("Is subset Customer Claim "+isSubset2("Customer","Claim"));
        //  System.out.println("getJoininTupe"+getJoiningTuple("Policy","Customer").toString());
        // System.out.println("getJoiningTuple2"+getJoiningTuple2("Policy","Customer").toString());
       // System.out.println("Septet "+septet.toString());

        SEPTET.forEach((k,v)->{
            System.out.println("Septet%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%55");
            System.out.println("Key "+k);
            System.out.println("Value "+v);
        });

       // TestCases.getMaxCardinalityTuple2Test();

        // TestCases.isMatcgProperSubsetTest();
        //Payment1&Claim1
        //Customer&Policy1
        //isSubset("Payment1&Claim1","Customer&Policy1");

        //System.out.println("Right Key "+getRightKey("Customer&Policy"));
        //System.out.println("Left key "+getLeftKey("Customer&Policy"));

        //		String sourceTopicList = "STATPEJ.POC_CLAIM,STATPEJ.POC_CLAIMPAYMENT,STATPEJ.POC_CUSTOMER,STATPEJ.POC_POLICY";
//		int consumerGroupId = 127;
//		long targetTotalCount = 50000;
         //HashMap<String,Object> hashMap=PopulateSampledData.populateHLLObjects("STATPEJ.POC_CLAIM,STATPEJ.POC_CLAIMPAYMENT,STATPEJ.POC_CUSTOMER,STATPEJ.POC_POLICY",135,50000);
         System.out.println(schemas.get("STATPEJ.POC_CUSTOMER"));
         System.out.println(schemas.get("STATPEJ.POC_POLICY"));
        //Schema schema= SchemaGeneratorUtils.generateJoinedSchema(schemas.get("STATPEJ.POC_CUSTOMER"),"STATPEJ.POC_CUSTOMER",schemas.get("STATPEJ.POC_POLICY"),"STATPEJ.POC_POLICY");
        //System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@22222");
        //System.out.println("schema"+schema.toString());
        // System.out.println("Schema "+schema.toString());
        /**@SuppressWarnings("unchecked")
        HashMap<String,String> schemas = (HashMap<String,String>)hashMap.get("schemaCollection");
        @SuppressWarnings("unchecked")
        ArrayList<HashMap<String, HashMap<Set<String>, HLL>>> hllCollection2 = (ArrayList<HashMap<String, HashMap<Set<String>, HLL>>> ) hashMap.get("hllCollection");

        hllCollection2.forEach((a) -> {

                    Set<String> setAName = a.keySet();
                    System.out.println("KeySet2 " + a.values().toString());
                });**/

        //System.out.println(schemas.get("STATPEJ.POC_CLAIM"));

        System.out.println("################&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
        //System.out.println(schemas.toString());
        System.out.println("getKey "+NeoUtils.getKey("STATPEJ.POC_CLAIMPAYMENT1&STATPEJ.POC_CLAIM1"));
        System.out.println("@@@@@@SCHEMAAAAAAAAASSSSSSSSSS");
        schemas.forEach((k,v)->{

            System.out.println("Key "+k);
            System.out.println("Value "+v);
        });
        System.out.println("@@@@@@SCHEMAAAAAAAAASSSSSSSSSS");

       /** NODES.forEach((e)-> {
            //System.out.println("SCHEMA "+e+"---"+schemas.get(e));

            if(JOINORREGROUPTRIPLET.get(e)!=null) {
                if (!(boolean) JOINORREGROUPTRIPLET.get(e).getValue2()) {
                    //false is a join
                    String left = JOINORREGROUPTRIPLET.get(e).getValue0().toString();
                    String right = JOINORREGROUPTRIPLET.get(e).getValue1().toString();
                    String leftSchema = schemas.get(left);
                    String rightSchema = schemas.get(right);
                    System.out.println("left "+left);
                    System.out.println("right "+right);
                    System.out.println("rightSchema "+rightSchema);
                    System.out.println("leftSchema "+leftSchema);

                   System.out.println("Schema " + SchemaGeneratorUtils.generateJoinedSchema(leftSchema, left, rightSchema, right));

                }
            }

        });**/

      /**  SEPTET.forEach((k,v)->{
            System.out.println("Key "+k);
            System.out.println("Value "+v);

        });**/

     System.out.println("GETObjectName "+NeoUtils.getObjectName("STATPEJ.POC_CLAIMPAYMENT21AndSTATPEJ.POC_CLAIM21"));

    }//main
}//class