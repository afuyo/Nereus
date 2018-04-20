package com.nereus;
import static com.nereus.HLLDB.*;

public class SetInfo {
    static Integer customerLines = 0;//493;
    static Integer policyLines = 0;// 597;
    static Integer claimLines = 0;//448;
    static Integer paymentLines = 0;// 395;

    static int getCardinality(String dataSetName)
    {

        switch(dataSetName){
            case CUSTOMER_TOPIC: return customerLines;

            case POLICY_TOPIC: return policyLines;

            case CLAIM_TOPIC: return claimLines;

            case PAYMENT_TOPIC: return paymentLines;

            default: return 0;

        }

    }
}
