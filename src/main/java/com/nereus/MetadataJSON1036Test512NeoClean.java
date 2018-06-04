package com.nereus;


import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.nereus.Utils.NeoUtils;
import com.nereus.Utils.Output;
import net.agkn.hll.HLL;
import org.apache.commons.collections4.MultiMap;
import org.javatuples.Pair;
import org.javatuples.Septet;
import org.javatuples.Sextet;
import org.javatuples.Triplet;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import scala.collection.immutable.Stream;

import java.io.*;
import java.util.*;

import static com.nereus.AvroUtils.Constants.*;
import static com.nereus.HLLDB.*;
//import java.lang.reflect.Array;

/**Adding second iteration **/

public class MetadataJSON1036Test512NeoClean {





    /**********************************************************************/

    /************************************************************************/

    /**public static String customerTopic = "Customer";
    static String policyTopic = "Policy";
    static String claimTopic = "Claim";
    static String paymentTopic = "Payment";
    static String addressTopic = "Address";**/

    //public static Triplet<String,String,Boolean> defaultTriplet = Triplet.with("Missing","Missing",false);

    static public class Customer {
        public String CUSTOMER;
        public String POLICY;
        public Double CUSTOMERTIME;
        public String ADDRESS;

        public Integer getHashCode(Set<String> sets)
        {
           int hashCode =0;
            ArrayList<Integer> hashCodes = new ArrayList<>();
           // System.out.println("Size"+sets.size());
            sets.forEach((s)->{

                switch (s)
                {
                    case "customer":
                         hashCodes.add(CUSTOMER.hashCode());
                        // System.out.println("Got customer");
                         break;
                    case "policy":
                        hashCodes.add(POLICY.hashCode());
                        //System.out.println("Got policy");
                        break;
                    case "customertime":
                        hashCodes.add(CUSTOMERTIME.hashCode());
                        //System.out.println("Got customertime");
                        break;
                    case "address":
                       hashCodes.add(ADDRESS.hashCode());
                       //System.out.println("Got address");
                       break;
                    default:
                        //System.out.println("Emmtpy Set");
                        break;
                }
            });
            Iterator<Integer> iter = hashCodes.iterator();
            while(iter.hasNext()){
                hashCode=hashCode+iter.next();
            }
            return hashCode;
        }
    }

    static public class Policy {
        public String POLICY;
        public Double POLICYSTARTTIME;
        public Double POLICYENDTIME;
        public String PVAR0;
        public String PVAR1;

        public Integer getHashCode(Set<String> sets)
        {
            int hashCode =0;
            ArrayList<Integer> hashCodes = new ArrayList<>();
            // System.out.println("Size"+sets.size());
            sets.forEach((s)->{

                switch (s)
                {
                    case "policy":
                        hashCodes.add(POLICY.hashCode());
                        break;
                    case "policystarttime":
                        hashCodes.add(POLICYSTARTTIME.hashCode());
                        break;
                    case "policyendtime":
                        hashCodes.add(POLICYENDTIME.hashCode());
                        break;
                    case "pvar0":
                        hashCodes.add(PVAR0.hashCode());
                        break;
                    case "pvar1":
                        hashCodes.add(PVAR1.hashCode());
                        break;
                    default:
                        break;
                }
            });
            Iterator<Integer> iter = hashCodes.iterator();
            while(iter.hasNext()){
                hashCode=hashCode+iter.next();
            }
            return hashCode;
        }
    }
    static public class Claim {

        public String CLAIMNUMBER;
        public Double CLAIMTIME;
        public Double CLAIMREPORTTIME;
        public String CLAIMCOUNTER;
        public String POLICY;

        public Integer getHashCode(Set<String> sets)
        {
            int hashCode =0;
            ArrayList<Integer> hashCodes = new ArrayList<>();
            // System.out.println("Size"+sets.size());
            sets.forEach((s)->{

                switch (s)
                {
                    case "claimnumber":
                        hashCodes.add(CLAIMNUMBER.hashCode());
                        break;
                    case "claimtime":
                        hashCodes.add(CLAIMTIME.hashCode());
                        break;
                    case "claimreporttime":
                        hashCodes.add(CLAIMREPORTTIME.hashCode());
                        break;
                    case "claimcounter":
                        hashCodes.add(CLAIMCOUNTER.hashCode());
                        break;
                    case "policy":
                        hashCodes.add(POLICY.hashCode());
                    default:
                        break;
                }
            });
            Iterator<Integer> iter = hashCodes.iterator();
            while(iter.hasNext()){
                hashCode=hashCode+iter.next();
            }
            return hashCode;
        }
    }
    static public class Payment {
        public String CLAIMNUMBER;
        public String CLAIMCOUNTER;
        public Double PAYTIME;
        public Double PAYMENT;


        public Integer getHashCode(Set<String> sets)
        {
            int hashCode =0;
            ArrayList<Integer> hashCodes = new ArrayList<>();
            // System.out.println("Size"+sets.size());
            sets.forEach((s)->{

                switch (s)
                {
                    case "claimnumber":
                        hashCodes.add(CLAIMNUMBER.hashCode());
                        break;
                    case "claimcounter":
                        hashCodes.add(CLAIMCOUNTER.hashCode());
                        break;
                    case "paytime":
                        hashCodes.add(PAYTIME.hashCode());
                        break;
                    case "payment":
                        hashCodes.add(PAYMENT.hashCode());
                        break;

                    default:
                        break;
                }
            });
            Iterator<Integer> iter = hashCodes.iterator();
            while(iter.hasNext()){
                hashCode=hashCode+iter.next();
            }
            return hashCode;
        }

    }

    public static Set<Set<String>> createTags(String tagsType){

        ArrayList<String> tagsArray = new ArrayList<>();
        String type = tagsType;
        switch(type)
        {
            case "Customer":
            tagsArray.add("customer");
            tagsArray.add("policy");
            tagsArray.add("customertime");
            tagsArray.add("address");
            break;
            case "Policy":
                tagsArray.add("policy");
                tagsArray.add("policystarttime");
                tagsArray.add("policyendtime");
                tagsArray.add("pvar0");
                tagsArray.add("pvar1");
            break;
            case "Claim":
                tagsArray.add("claimnumber");
                tagsArray.add("claimtime");
                tagsArray.add("claimreporttime");
                tagsArray.add("claimcounter");
                tagsArray.add("policy");
                break;
            case "Payment":
                tagsArray.add("claimnumber");
                tagsArray.add("claimcounter");
                tagsArray.add("paytime");
                tagsArray.add("payment");
            default:
                break;

        }


        Set<String> tags =  new HashSet<>();
        tags.addAll(tagsArray);
        Set<Set<String>> tagsSets = Sets.powerSet(tags);
        return tagsSets;

        //for (String hashtag : tagsForQuery) {
          //  System.out.println(hashtag);
      //  }
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
           next=NODES.size();
       }

   }

    public static void main(String[] arg) throws IOException, CloneNotSupportedException {
      //  System.out.println("Hello");
        //System.out.println(SetInfo.customerLines);

        HashFunction hashFunction = Hashing.murmur3_128();
        ArrayList<GraphNode> graph = new ArrayList<>();
        HashMap<String,String> setsTable = new HashMap();



        /*****CREATE TEST DATA**************/






        BufferedReader br = new BufferedReader(new FileReader("C:\\githubrepos\\Nereus\\files\\CustomerFormatted.txt"));
        //BufferedReader br = new BufferedReader(new FileReader("/home/statarm/playground/Code/tomekl007-packt_algo-44b37f423231/customerTest1.txt"));
        //BufferedReader br = new BufferedReader(new FileReader("/home/statarm/playground/Code/tomekl007-packt_algo-44b37f423231/customerTest2.txt"));
        //BufferedReader br = new BufferedReader(new FileReader("/home/statarm/playground/Code/tomekl007-packt_algo-44b37f423231/customer1.txt"));

        ArrayList<Customer> A = new ArrayList<>();
        int i =0;
        for (String line; (line=br.readLine()) !=null;)
        {
            i++;
            JSONObject objCus = new JSONObject(line);
            Customer cust = new Customer();
            cust.CUSTOMER=objCus.getString("CUSTOMER");
            cust.POLICY=objCus.getString("POLICY");
            cust.CUSTOMERTIME=objCus.getDouble("CUSTOMERTIME");
            cust.ADDRESS=objCus.getString("ADDRESS");
            A.add(cust);

        }
      // System.out.println("Number of customer lines "+i);
        SetInfo.customerLines=i;

        System.out.println("Customer Lines "+ SetInfo.customerLines);

        br = new BufferedReader(new FileReader("C:\\githubrepos\\Nereus\\files\\PolicyFormatted.txt"));
        //br = new BufferedReader(new FileReader("/home/statarm/playground/Code/tomekl007-packt_algo-44b37f423231/policyTest11.txt"));
      // br = new BufferedReader(new FileReader("/home/statarm/playground/Code/tomekl007-packt_algo-44b37f423231/policyTest2.txt"));
        //br = new BufferedReader(new FileReader("/home/statarm/playground/Code/tomekl007-packt_algo-44b37f423231/policy1.txt"));

        ArrayList<Policy> B = new ArrayList<>();

        i=0;
        for(String line; (line=br.readLine()) !=null;)
        {
            i++;
            JSONObject objPol = new JSONObject(line);
            Policy policy = new Policy();
            policy.POLICY= objPol.getString("POLICY");
            policy.POLICYSTARTTIME = objPol.getDouble("POLICYSTARTTIME");
            policy.POLICYENDTIME  = objPol.getDouble("POLICYENDTIME");
            policy.PVAR0 = objPol.getString("PVAR0");
            policy.PVAR1 = objPol.getString("PVAR1");
            B.add(policy);
        }
        //System.out.println("Number of PolicyLines"+i);
        SetInfo.policyLines=i;

        br = new BufferedReader(new FileReader("C:\\githubrepos\\Nereus\\files\\ClaimFormatted.txt"));
        //br = new BufferedReader(new FileReader("/home/statarm/playground/Code/tomekl007-packt_algo-44b37f423231/claimTest1.txt"));
       // br = new BufferedReader(new FileReader("/home/statarm/playground/Code/tomekl007-packt_algo-44b37f423231/claim1.txt"));

        ArrayList<Claim> C = new ArrayList<>();

        i=0;
        for(String line; (line=br.readLine()) !=null;)
        {
            i++;
            JSONObject objClaim = new JSONObject(line);
            Claim claim = new Claim();
            claim.CLAIMNUMBER= objClaim.getString("CLAIMNUMBER");
            claim.CLAIMTIME = objClaim.getDouble("CLAIMTIME");
            claim.CLAIMREPORTTIME = objClaim.getDouble("CLAIMREPORTTIME");
            claim.CLAIMCOUNTER = objClaim.getString("CLAIMCOUNTER");
            claim.POLICY = objClaim.getString("CLAIMNUMBER").split("_")[0];

            //objClaim.get("CLAIMNUMBER").toString().split("_")[0];
            C.add(claim);
        }

        //System.out.println("Number of ClaimLines"+i);
        SetInfo.claimLines=i;

        br = new BufferedReader(new FileReader("C:\\githubrepos\\Nereus\\files\\PaymentFormatted.txt"));
      // br = new BufferedReader(new FileReader("/home/statarm/playground/Code/tomekl007-packt_algo-44b37f423231/paymentTest1.txt"));
        //br = new BufferedReader(new FileReader("/home/statarm/playground/Code/tomekl007-packt_algo-44b37f423231/payment1.txt"));

        ArrayList<Payment> D = new ArrayList<>();

        i=0;
        for(String line; (line=br.readLine()) !=null;)
        {
            i++;
            JSONObject objPayment = new JSONObject(line);
            Payment payment = new Payment();
            payment.CLAIMNUMBER= objPayment.getString("CLAIMNUMBER");
            payment.CLAIMCOUNTER = objPayment.getString("CLAIMCOUNTER");
            payment.PAYTIME = objPayment.getDouble("PAYTIME");
            payment.PAYMENT = objPayment.getDouble("PAYMENT");

            D.add(payment);
        }


        SetInfo.paymentLines=i;

        HashMap<Set<String>,HLL> custCardinalities= new HashMap<>();
        HashMap<Set<String>,HLL> polCardinalities  = new HashMap<>();
        HashMap<Set<String>,HLL> claimCardinalities = new HashMap<>();
        HashMap<Set<String>,HLL> payCardinalities = new HashMap<>();

        HashMap<String,HashMap<Set<String>,HLL>> custCardinalities2= new HashMap<>();
        HashMap<String,HashMap<Set<String>,HLL>> polCardinalities2= new HashMap<>();
        HashMap<String,HashMap<Set<String>,HLL>> claimCardinalities2= new HashMap<>();
        HashMap<String,HashMap<Set<String>,HLL>> payCardinalities2= new HashMap<>();



        custCardinalities2.put(CUSTOMER_TOPIC,new HashMap<>());
        polCardinalities2.put(POLICY_TOPIC,new HashMap<>());
        claimCardinalities2.put(CLAIM_TOPIC,new HashMap<>());
        payCardinalities2.put(PAYMENT_TOPIC,new HashMap());


        Set<Set<String>> customerTagSets = createTags(CUSTOMER_TOPIC);
        String s;
        for (Set<String> sets : customerTagSets) {
            // if(!sets.isEmpty()) {
            //  System.out.println("Tagsets " +sets);
            //s = "Customer " + sets;
            HLL hllA = new HLL(14, 5);
            A.forEach((a) -> {
                int hashKey = a.getHashCode(sets);
                // long hashedValue = hashFunction.newHasher().putInt(a.CUSTOMER.hashCode()+a.CUSTOMERTIME.hashCode()).hash().asLong();
                long hashedValue = hashFunction.newHasher().putInt(hashKey).hash().asLong();
                //hllA.addRaw(hashedValue);
                hllA.addRaw(hashedValue);
                custCardinalities.put(sets, hllA);
                custCardinalities2.get(CUSTOMER_TOPIC).put(sets, hllA);

            });
            //}
        }

        // System.out.println("Number of PaymentLines"+i);



        Set<Set<String>> polTagSets = createTags(POLICY_TOPIC);


        for (Set<String> sets : polTagSets) {
            // System.out.println("Tagsets " +sets);
            // if(!sets.isEmpty()) {
            s = "Policy " + sets;
            HLL hllB = new HLL(14, 5);
            B.forEach((b) -> {
                int hashKey = b.getHashCode(sets);

                long hashedValue = hashFunction.newHasher().putInt(hashKey).hash().asLong();

                hllB.addRaw(hashedValue);
                polCardinalities.put(sets, hllB);
                polCardinalities2.get(POLICY_TOPIC).put(sets, hllB);


            });
            //  }

            //System.out.println(s+" Cardinality "+hllB.cardinality());
        }


        Set<Set<String>> claimTagSets = createTags(CLAIM_TOPIC);


        for (Set<String> sets : claimTagSets) {
            //  if(!sets.isEmpty()) {
            // System.out.println("Tagsets " +sets);
            s = "Claim " + sets;
            HLL hllC = new HLL(14, 5);
            C.forEach((c) -> {
                //System.out.println("ClaimPolicy "+c.POLICY);
                //System.out.println("ClaimCalimnumber "+c.CLAIMNUMBER);
                int hashKey = c.getHashCode(sets);

                long hashedValue = hashFunction.newHasher().putInt(hashKey).hash().asLong();

                hllC.addRaw(hashedValue);
                claimCardinalities.put(sets, hllC);
                claimCardinalities2.get(CLAIM_TOPIC).put(sets, hllC);


            });
            //  }

            // System.out.println(s+" Cardinality "+hllC.cardinality());
        }

        Set<Set<String>> payTagSets = createTags(PAYMENT_TOPIC);






        for (Set<String> sets : payTagSets) {

            // if(!sets.isEmpty()) {
            // System.out.println("Tagsets " + sets);

            HLL hllD = new HLL(14, 5);
            D.forEach((d) -> {
                int hashKey = d.getHashCode(sets);

                long hashedValue = hashFunction.newHasher().putInt(hashKey).hash().asLong();

                hllD.addRaw(hashedValue);
                payCardinalities.put(sets, hllD);
                payCardinalities2.get(PAYMENT_TOPIC).put(sets, hllD);


            });
            // }
            //System.out.println(s+" Cardinality "+hllD.cardinality());
        }

        //Find intersection for Policy and customer

        //Collection of HLLs


        ArrayList <HashMap<String,HashMap<Set<String>,HLL>>> hllCollection = new ArrayList<>();
        hllCollection.add(custCardinalities2);
        hllCollection.add(polCardinalities2);
        hllCollection.add(claimCardinalities2);
        hllCollection.add(payCardinalities2);

        //findCommonKeys(custCardinalities,polCardinalities);
        // findCommonKeys(claimCardinalities,payCardinalities);
        //findCommonKeys(polCardinalities,claimCardinalities);



        hllCollection.forEach((a) -> {

            Set<String> setAName = a.keySet();
            System.out.println("KeySet "+a.values().toString());
            hllCollection.forEach((b)->{
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
                        //Builder.findCommonKeys3(a, b,setAName,setBName);
                        Builder.fit(a, b,setAName,setBName);

                    } //catch( CloneNotSupportedException e)
                    catch(Exception e)
                    {
                        e.printStackTrace();
                    }

                }


            });
        });

        generateGraph();


        /**System.out.println("JoinTrippeltXXXXXXXXXXXXXXXXXXXXXx");
        joinTriplet.forEach((k,v)->{
            if((Boolean) v.getValue2() ) {
                System.out.println(k.toString());
                System.out.println(v.toString());
                //System.out.println(k.contains("Payment1"));
                //System.out.println(v.contains("Payment1"));
                // System.out.println(joinTriplet.keySet().contains("Payment1"));
            }
        });**/



        System.out.println("Septet");



        SEPTET.forEach((k,v)->{
            System.out.println(k.toString()+" "+v.toString());

        });

        //writeToCSV();
       // NeoUtils.writeToNeo2();
        NeoUtils.writeToNeo3();


        //System.out.println("TempRegrouped "+(tempRegrouped.hashCode()));
        //System.out.println("TempRegrouped "+(tempRegrouped.toString()));
        /**System.out.println("Idenfied by "+(IDENTIFIEDBY.hashCode()));
        System.out.println("Idenfied by "+(IDENTIFIEDBY.toString()));
        System.out.println("Septetst are equal "+(septet.hashCode()==TestClass.septet));
        System.out.println("Supersets are equal "+(superset.hashCode()==TestClass.superset));
        System.out.println("Subsets are equal "+(subset.hashCode()==TestClass.subset));
        System.out.println("Overlap is equal "+(overlap.hashCode()==TestClass.overlap));
        System.out.println("JointTriplet is equal "+(joinTriplet.hashCode()==TestClass.jointriplet));
        System.out.println("Identified by "+(IDENTIFIEDBY.hashCode()==TestClass.identifiedBy));**/


        //  System.out.println("Get joining tuple for Customer & Claim"+getJoiningTuple("Customer","Claim").toString());
        //System.out.println("Is subset Customer Claim "+isSubset2("Customer","Claim"));
        //  System.out.println("getJoininTupe"+getJoiningTuple("Policy","Customer").toString());


       // TestCases.getMaxCardinalityTuple2Test();

        // TestCases.isMatcgProperSubsetTest();
        //Payment1&Claim1
        //Customer&Policy1
        //isSubset("Payment1&Claim1","Customer&Policy1");

         System.out.println("Right Key "+NeoUtils.getRightKey("Customer&Policy"));
        System.out.println("Left key "+NeoUtils.getLeftKey("Customer&Policy"));

     /**   System.out.println("JOIN START END");
        joinsStartEnd.forEach((k,v)->{

            System.out.println("Key "+k);
            System.out.println("Value "+v);

        });**/

      /**  System.out.println("Policy "+getJoinEnd2("Policy"));
        System.out.println("Customer "+getJoinEnd2("Customer"));
        System.out.println("Customer&Policy "+getJoinEnd2("Customer&Policy"));
        System.out.println("Customer&Policy1 "+getJoinEnd2("Customer&Policy1"));**/

      /**System.out.println("StreamsEnd ");
      System.out.println("StreamStart"+getStreamStart2("Payment1&Claim"));
      System.out.println("StreamEnd "+getStreamEnd("Payment1&Claim"));

        System.out.println("StreamStart2"+getStreamStart2("Payment1&Claim1"));
        System.out.println("StreamEnd2 "+getStreamEnd("Payment1&Claim1"));**/

       /** System.out.println("StreamsEnd ");
        System.out.println("StreamStart should be Cusomter&Policy");
        System.out.println("StreamStart"+NeoUtils.getStreamStart2("Customer&Policy"));
        System.out.println("StreamEnd should be Customer&Policy1");
        System.out.println("StreamEnd "+NeoUtils.getStreamEnd("Customer&Policy"));

        System.out.println("StreamStart should be Cusomter&Policy");
        System.out.println("StreamStart"+NeoUtils.getStreamStart("Customer&Policy"));
        System.out.println("StreamEnd should be Customer&Policy1");
        System.out.println("StreamEnd "+NeoUtils.getStreamEnd("Customer&Policy"));



        System.out.println("StreamStart2"+NeoUtils.getStreamStart2("Customer&Policy1"));
        System.out.println("StreamEnd2 "+NeoUtils.getStreamEnd("Customer&Policy1"));**/




      /**  System.out.println("Transformed start end ");
        transformsStartEnd.forEach((k,v)->{
            System.out.println("Key "+k);
            System.out.println("Value "+v);
        });

        System.out.println("Transformed2 start end ");
         transformsStartEnd2.forEach((k,v)->{
         System.out.println("Key "+k);
         System.out.println("Value "+v);
         });**/

    }//main
}//class