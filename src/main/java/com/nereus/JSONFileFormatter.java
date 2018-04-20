package com.nereus;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

//import jdk.nashorn.internal.parser.JSONParser;
//import org.json.JSONObject;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

public class JSONFileFormatter {

    public static void main(String[] args) throws IOException, ParseException {
       String filePath = "/home/statarm/playground/Nereus/files/";
       String customerSource= filePath+"PocCustomer2.txt";
       String customerTarget= filePath+"PocCustomer2FormattedX.txt";
       String policySource = filePath+"PocPolicy2.txt";
       String policyTarget = filePath+"PocPolicy2FormattedX.txt";
       String claimSource = filePath+"PocClaim2.txt";
       String claimTarget = filePath+"PocClaim2FormattedX.txt";
       String paymentSource = filePath+"PocClaimPayment2.txt";
       String paymentTarget = filePath+"PocClaimPayment2FormattedX.txt";
       String addressSource = filePath+"PocAddress2.txt";
       String addressTarget = filePath+"PocAddress2FormattedX.txt";

      /** PrintWriter pw = new PrintWriter(new File("/home/statarm/playground/Nereus/files/PocCustomer2FormattedX.txt"));

        JSONParser jsonParser = new JSONParser();
        File file2 = new File("/home/statarm/playground/Nereus/files/PocCustomer2.txt");
        FileReader fileReader = new FileReader(file2);
        Object object = jsonParser.parse(new FileReader(file2));
        ArrayList<String> lines = new ArrayList<>();

        JSONObject jsonObject = (JSONObject) object;
        JSONArray jsonArray  = (JSONArray)jsonObject.get("data");

        System.out.println("Array   "+jsonArray);
        StringBuilder sb = new StringBuilder();
        jsonArray.forEach((e)->{
            System.out.println(e.toString());
            sb.append(e);
            sb.append("\r");
        });
       pw.write(sb.toString());
       pw.close();**/

       writeFormattedJson(customerSource,customerTarget);
       writeFormattedJson(policySource,policyTarget);
       writeFormattedJson(claimSource,claimTarget);
       writeFormattedJson(paymentSource,paymentTarget);
       writeFormattedJson(addressSource,addressTarget);




    }
    public static void writeFormattedJson(String sourcePath,String targetPath) throws FileNotFoundException, ParseException, IOException
    {

        PrintWriter pw = new PrintWriter(new File(targetPath));

        JSONParser jsonParser = new JSONParser();
        File file2 = new File(sourcePath);
        FileReader fileReader = new FileReader(file2);
        Object object = jsonParser.parse(new FileReader(file2));
        ArrayList<String> lines = new ArrayList<>();

        JSONObject jsonObject = (JSONObject) object;
        JSONArray jsonArray  = (JSONArray)jsonObject.get("data");

        System.out.println("Array   "+jsonArray);
        StringBuilder sb = new StringBuilder();
        jsonArray.forEach((e)->{
            System.out.println(e.toString());
            sb.append(e);
            sb.append("\r");
        });
        pw.write(sb.toString());
        pw.close();

    }

    public static void getArray(Object object2) throws ParseException {

        JSONArray jsonArr = (JSONArray) object2;

        for (int k = 0; k < jsonArr.size(); k++) {

            if (jsonArr.get(k) instanceof JSONObject) {
                parseJson((JSONObject) jsonArr.get(k));
            } else {
                System.out.println(jsonArr.get(k));
            }

        }
    }

    public static void parseJson(JSONObject jsonObject) throws ParseException {

        Set<String> set = jsonObject.keySet();
        Iterator<String> iterator = set.iterator();
        while (iterator.hasNext()) {
            Object obj = iterator.next();
            if (jsonObject.get(obj) instanceof JSONArray) {
                System.out.println(obj.toString());
                getArray(jsonObject.get(obj));
            } else {
                if (jsonObject.get(obj) instanceof JSONObject) {
                    parseJson((JSONObject) jsonObject.get(obj));
                } else {
                    System.out.println(obj.toString() + "\t"
                            + jsonObject.get(obj));
                }
            }
        }
    }

}
