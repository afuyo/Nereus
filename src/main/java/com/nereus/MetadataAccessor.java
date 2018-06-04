package com.nereus;

import org.apache.commons.collections4.MultiMap;
import org.apache.commons.collections4.map.MultiValueMap;
import org.javatuples.Sextet;
import org.javatuples.Triplet;

import java.util.Collection;
import java.util.Set;

import static com.nereus.HLLDB.*;

public class MetadataAccessor {


    /**
     * Return collection of proper subsets for a dataset.
     * @param dataSetName
     * @return
     */
    public static Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> getProperSubset(String dataSetName)
    {
        Collection  properSubset = (Collection) PROPERSUBSET.get(dataSetName);
        return properSubset;

    }

    /**
     * Return collection containing superset tuples
     * @param dataSetName
     * @return
     */
    public static Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>>  getSuperset( String dataSetName){
        Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> superSet= (Collection) SUPERSET.get(dataSetName);
        return superSet;
    }

    /**
     * Return collection containing subset tuples
     * @param dataSetName
     * @return
     */
    public static Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>>  getSubset( String dataSetName){
        Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> superSet= (Collection) SUBSET.get(dataSetName);
        return superSet;
    }

    /**
     * Return joined tuple.
     * @param dataSetName
     * @return
     */

    public static Triplet<String,String,Boolean> getJoinedTuple(String dataSetName)
    {
        Triplet<String,String,Boolean> triplet = JOINTRIPLET.get(dataSetName);
        return triplet;
    }

    /**
     * Return all tuples part of join or regroup.
     * @return
     */
    public  static Collection<Triplet<String,String,Boolean>> getJoinedTuples()
    {
        return  (Collection) JOINTRIPLET.values();

    }

    /**
     * Return all datasets names that are are part of join or regroup.
     * @return
     */

    public static Set<String> getDataSetsWithDescendants()
    {
        return JOINTRIPLET.keySet();
    }
    public static Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>>  getConsumedTuples()
    {
        return (Collection) SEPTET.values();
    }


    /**
     * Label operation as used in a join.
     * @param dataSetName
     * @param consumedTuple
     */
    public static void putConsumedTuple(String dataSetName,Sextet<String, Set<String>, Long, String, Set<String>, Long> consumedTuple){

        if(!SEPTET.containsValue(consumedTuple))
        {
            SEPTET.put(dataSetName,consumedTuple);
        }


    }

    /**
     * Insert superset tuple
     * @param dataSetName
     * @param superSetTuple
     */

    public static void putSupersetTuple(String dataSetName,Sextet<String, Set<String>, Long, String, Set<String>, Long> superSetTuple )
    {
        if(superSetTuple!=null) {
            SUPERSET.put(dataSetName, superSetTuple);
        }
    }

}
