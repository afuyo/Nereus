package com.nereus;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.collections4.MultiMap;
import org.apache.commons.collections4.map.MultiValueMap;
import org.javatuples.Pair;
import org.javatuples.Sextet;
import org.javatuples.Triplet;

import java.util.*;

public class HLLDB {

    public static final String CUSTOMER_TOPIC ="STATPEJ.POC_CUSTOMER2";
   public static final String POLICY_TOPIC ="STATPEJ.POC_POLICY2";
    public static final String CLAIM_TOPIC  = "STATPEJ.POC_CLAIM2";
    public static final String PAYMENT_TOPIC = "STATPEJ.POC_CLAIMPAYMENT2";
    public static final String ADDRESS_TOPIC = "STATPEJ.POC_ADDRESS2";

/** USE THIS for avro schema stuff*/
   // static ArrayList<String> nodes = new ArrayList<>(Arrays.asList("STATPEJ.POC_CLAIM","STATPEJ.POC_CLAIMPAYMENT","STATPEJ.POC_CUSTOMER","STATPEJ.POC_POLICY"));
    //static ArrayList<String> streamnodes = new ArrayList<>(Arrays.asList("STATPEJ.POC_CLAIM","STATPEJ.POC_CLAIMPAYMENT","STATPEJ.POC_CUSTOMER","STATPEJ.POC_POLICY"));
   // static ArrayList<String> streamnodes = new ArrayList<>(Arrays.asList("ClaimSt","CustomerSt","PaymentSt","PolicySt"));
   // static ArrayList<String> tablenodes = new ArrayList<>(Arrays.asList("STATPEJ.POC_CLAIM","STATPEJ.POC_CLAIMPAYMENT","STATPEJ.POC_CUSTOMER","STATPEJ.POC_POLICY"));


    public static ArrayList<String> SOURCE_NODES= new ArrayList<>(Arrays.asList("STATPEJ.POC_CLAIM2","STATPEJ.POC_CLAIMPAYMENT2","STATPEJ.POC_CUSTOMER2","STATPEJ.POC_POLICY2","STATPEJ.POC_ADDRESS2"));
    public static ArrayList<String> NODES= new ArrayList<>(Arrays.asList("STATPEJ.POC_CLAIM2","STATPEJ.POC_CLAIMPAYMENT2","STATPEJ.POC_CUSTOMER2","STATPEJ.POC_POLICY2","STATPEJ.POC_ADDRESS2"));
    //static ArrayList<String> streamnodes = new ArrayList<>(Arrays.asList("STATPEJ.POC_CLAIM","STATPEJ.POC_CLAIMPAYMENT","STATPEJ.POC_CUSTOMER","STATPEJ.POC_POLICY"));
    public static ArrayList<String> STREAMNODES = new ArrayList<>(Arrays.asList("ClaimSt","CustomerSt","PaymentSt","PolicySt","AddressSt"));
    public static ArrayList<String> TABLENODES = new ArrayList<>(Arrays.asList("STATPEJ.POC_CLAIM2","STATPEJ.POC_CLAIMPAYMENT2","STATPEJ.POC_CUSTOMER2","STATPEJ.POC_POLICY2","STATPEJ.POC_ADDRESS2"));

    //static ArrayList<String> nodes = new ArrayList<>(Arrays.asList("POC_CLAIMList","POC_CLAIMPAYMENTList","POC_CUSTOMERList","POC_POLICYList"));
    //static ArrayList<String> streamnodes = new ArrayList<>(Arrays.asList("POC_CLAIM","POC_CLAIMPAYMENT","POC_CUSTOMER","POC_POLICY"));
    //static ArrayList<String> tablenodes = new ArrayList<>(Arrays.asList("STATPEJ.POC_CLAIM","STATPEJ.POC_CLAIMPAYMENT","STATPEJ.POC_CUSTOMER","STATPEJ.POC_POLICY"));
  /***USE THIS for Neo512 and Neo512Clean**/
  // static ArrayList<String> nodes = new ArrayList<>(Arrays.asList("Claim","Customer","Payment","Policy"));
   // static ArrayList<String> streamnodes = new ArrayList<>(Arrays.asList("ClaimSt","CustomerSt","PaymentSt","PolicySt"));
   // static ArrayList<String> tablenodes = new ArrayList<>(Arrays.asList("Claim","Customer","Payment","Policy"));
    //static ArrayList<String> nodes = new ArrayList<>(Arrays.asList("Claim","Customer","Payment","Policy","Address"));
    //static ArrayList<String> streamnodes = new ArrayList<>(Arrays.asList("Claim","Customer","Payment","Policy","Address"));
    //static ArrayList<String> tablenodes = new ArrayList<>(Arrays.asList("Claim","Customer","Payment","Policy","Address"));
   /***                  ****************************/
    public static ArrayList<String> TEMPREGROUPED = new ArrayList<>();
    public static ArrayList<String> TEMPJOINS = new ArrayList<>();
    public static MultiMap<String,String> IDENTIFIEDBY = new MultiValueMap<>();
    public static HashMap<String,Set<String>> IDENTIFIEDBY2 = new HashMap<>();
    public static MultiMap<String,Sextet> SUPERSET = new MultiValueMap<>();
    public static MultiMap<String,Sextet> SUBSET = new MultiValueMap<>();
    public static MultiMap<String,Sextet<String,Set<String>,Long,String,Set<String>,Long>> PROPERSUBSET = new MultiValueMap<>();
    public static MultiMap<String,Sextet> OVERLAP = new MultiValueMap<>();
    public static MultiMap<String,Sextet> SEPTET = new MultiValueMap<>();
    //shows all joins and regroups if true regroug else join
    public static HashMap<String,Triplet> JOINTRIPLET = new HashMap<>();


    /**Graph Metadata ********************************************/
    //contains joins and regroups, true if regroup. <newTablename,<lname,rname,TRUE/FALSE>
    //public static LinkedHashMap<String,Triplet> JOINORREGROUPTRIPLET = new LinkedHashMap<>();
    public static Multimap<String,Triplet> JOINORREGROUPTRIPLET = LinkedListMultimap.create();
    public static MultiMap<String,Sextet> JOINS = new MultiValueMap<>();
    public static HashMap<String,String> JOINSSTARTEND = new HashMap<>();
    public static HashMap<String,Pair> JOINSSTARTEND2 = new HashMap<>();
    public static HashMap<String,String> TRANSFORMSTARTEND = new HashMap<>();
    public static HashMap<String,String> TRANSFORMSTARTEND2 = new HashMap<>();
    public static HashMap<String,String> AVRO_SCHEMAS = new HashMap<>();
    public static HashMap<String,String> streamTableMapping = new HashMap<>();
    static {
       /** streamTableMapping.put("Claim","ClaimSt");
        streamTableMapping.put("Customer","CustomerSt");
        streamTableMapping.put("Payment","PaymentSt");
        streamTableMapping.put("Policy","PolicySt");**/

        /**streamTableMapping.put("STATPEJ.POC_CLAIM","ClaimSt");
        streamTableMapping.put("STATPEJ.POC_CUSTOMER","CustomerSt");
        streamTableMapping.put("STATPEJ.POC_CLAIMPAYMENT","PaymentSt");
        streamTableMapping.put("STATPEJ.POC_POLICY","PolicySt");**/

        /**ADDRESS*/
        streamTableMapping.put("STATPEJ.POC_CLAIM2","ClaimSt");
        streamTableMapping.put("STATPEJ.POC_CUSTOMER2","CustomerSt");
        streamTableMapping.put("STATPEJ.POC_CLAIMPAYMENT2","PaymentSt");
        streamTableMapping.put("STATPEJ.POC_POLICY2","PolicySt");
        streamTableMapping.put("STATPEJ.POC_ADDRESS2","AddressSt");
    }
    public static HashMap<String,String> streamTopicMapping = new HashMap();
    static {


        /**ADDRESS*/
        streamTableMapping.put("ClaimSt","STATPEJ.POC_CLAIM2");
        streamTableMapping.put("CustomerSt","STATPEJ.POC_CUSTOMER2");
        streamTableMapping.put("PaymentSt","STATPEJ.POC_CLAIMPAYMENT2");
        streamTableMapping.put("PolicySt","STATPEJ.POC_POLICY2");
        streamTableMapping.put("AddressSt","STATPEJ.POC_ADDRESS2");
    }



    /**********************************************************************/
}
