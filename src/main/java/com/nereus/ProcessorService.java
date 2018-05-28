package com.nereus;

import org.javatuples.Sextet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import static com.nereus.HLLDB.*;

public class ProcessorService {

    public static boolean supersetContainsTable(String leftName,String rightName) {
        Boolean contains = false;
        Collection<Sextet> superSets = (Collection) SUPERSET.get(leftName);
        if (superSets != null) {
            Iterator<Sextet> iter = superSets.iterator();

            while (iter.hasNext()) {
                if (iter.next().getValue3().equals(rightName)) {
                    contains = true;
                   // break; //TODO Customer having policy&&adress in superset will get false with argurement leftname=Customer rightName=Policy. false on second itereation althoug true
                    //TODO this entire superset - superse/subset/propersubset(first condition in join)  needs rethinking and is not used at the moment
                } else {
                    contains = false;
                }
            }

        }
        return contains;
    }

    public static void removeRelations(String leftName, String rightName) {
        //superset on the right
        /** Superset - subset relationship has been identified and materialized in a join
         * We need to keep truck of it and move these relations to a pool of used relationships.
         */
        Collection<Sextet<String, Set<String>, Long, String, Set<String>, Long>> superX = (Collection) SUPERSET.get(rightName);
        Collection<Sextet<String, Set<String>, Long, String, Set<String>, Long>> subX = (Collection) SUBSET.get(leftName);




        //Use set to capture only one relationship.


        if (superX != null) {

            superX.forEach((e) -> {
                if (e.getValue0().equals(rightName) & e.getValue3().equals(leftName)) {

                    if (!SEPTET.containsValue(e)) {
                        SEPTET.put(rightName, e);
                    }
                }


            });

            if (subX != null) {
                subX.forEach((e) -> {
                    if (e.getValue0().equals(leftName) & e.getValue3().equals(rightName)) {

                        if (!SEPTET.containsValue(e)) {
                            SEPTET.put(leftName, e);
                        }
                    }


                });
            }
        }
    }

    public static void transferRelations(String subName, String superName, String newName) {
        //copy supersets, propersubset and overlaps from the right talb
        //supersets only if not contains current left and right

        Collection<Sextet<String, Set<String>, Long, String, Set<String>, Long>> superX = (Collection) SUPERSET.get(superName);
         if(superX!=null) {
             superX.forEach((e) -> {

                 if (!e.getValue3().equals(subName) && !SEPTET.containsValue(e)) {
                     //System.err.println("subName"+subName+" superName "+superName+" newName "+newName);
                     //System.err.println(e);
                     SUPERSET.put(newName, e);

                 }
             });
         }

        Collection<Sextet<String, Set<String>, Long, String, Set<String>, Long>> properSubX = (Collection) PROPERSUBSET.get(superName);

        if(properSubX!=null) {
            properSubX.forEach((e) -> {

                if (!e.getValue3().equals(subName) && !SEPTET.containsValue(e)) {
                    //System.err.println("subName" + subName + " superName " + superName + " newName " + newName);
                    //System.err.println(e);
                    PROPERSUBSET.put(newName, e);

                }
            });
        }

        Collection<Sextet<String, Set<String>, Long, String, Set<String>, Long>> overlapX = (Collection) OVERLAP.get(superName);

        if(overlapX!=null) {
            overlapX.forEach((e) -> {

                if (!e.getValue3().equals(subName)) {

                    OVERLAP.put(newName, e);

                }
            });
        }
    }

    public static Collection<Sextet> getJoiningTuple2(String leftName, String rightName)
    {
        Boolean contains = false;
        Collection<Sextet> col = (Collection) SUBSET.get(leftName);
        Collection<Sextet> colright = (Collection) SUPERSET.get(rightName);
        Collection<Sextet> properCol =  new ArrayList<>();

        //there is subset for that node
        if (col != null) {

            /** Iterator<Sextet> iter = col.iterator();
             while (iter.hasNext()) {

             Sextet sextet = iter.next();
             /**   System.out.println("sextet"+sextet);
             System.out.println("lname "+leftName);
             System.out.println("rname "+ rightName);**/

            /**  if (sextet.getValue3().equals(rightName))
             {
             contains= true;
             }**/



            //}
            //check if right matches left
            if(colright != null && colright.stream().anyMatch(e->e.getValue3().equals(col.iterator().next().getValue0()))) {
                // colright.stream().anyMatch(e->e.getValue3().equals(col.iterator().next().getValue0()));
                System.out.println("getJoininTuple2 left and right"+leftName+" "+rightName);
                //check if left matches right
                if(col.stream().anyMatch(s -> s.getValue3().equals(colright.iterator().next().getValue0()))) {
                    properCol.add(col.stream().filter(s -> s.getValue3().equals(colright.iterator().next().getValue0())).findFirst().get());
                }
                //contains = col.stream().anyMatch(s -> s.getValue3().equals(colright.iterator().next().getValue0()));

            }


        }
        return properCol;
    }




    /****OLD STUFF**/


    /**  public static boolean hasBeenJoined (String leftName, String rightName)
     {
     Boolean hasBeenJoined = false;

     Triplet triplet = joinTriplet.get(leftName);
     if (triplet!=null) {
     if (triplet.getValue0() == leftName & triplet.getValue1() == rightName ) {
     hasBeenJoined = true;
     }
     }

     return hasBeenJoined;
     }**/

    /** public boolean visited (String leftName, Sextet tuple)
     {
     Boolean visited = false;
     if (septet.containsValue(tuple))
     {
     visited = true;
     }

     return visited;
     }**/


    /**public static Collection<Sextet> getJoiningTuple(String leftName, String rightName)
    {

        Collection<Sextet> col = (Collection) subset.get(leftName);
        Collection<Sextet> properCol = new ArrayList<>();

        //there is subset for that node
        if (col != null ) {

            if( col.stream().anyMatch(s->s.getValue3().equals(rightName))){
                //Sextet<String,Set<String>,Long,String,Set<String>,Long> temp =
                //  col.stream().filter(s->s.getValue3().equals(rightName)).findFirst().get();
                properCol.add(col.stream().filter(s->s.getValue3().equals(rightName)).findFirst().get());
                return properCol;


            }
        }

        return null;

    }**/

}
