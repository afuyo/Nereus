package com.nereus;

import org.apache.commons.collections4.MultiMap;
import org.javatuples.Sextet;
import org.javatuples.Triplet;

import java.util.*;
import static com.nereus.HLLDB.*;

public class MetadataService {

    /**
     * determine whether there is superset propersubset realtion
     *
     * @param leftName String
     * @param rightName String
     */
    public static Boolean isInProperSubsetSuperset (String leftName, String rightName)
    {
        Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> leftProperSubset= (Collection) PROPERSUBSET.get(leftName);
        Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> rightSuperSet= (Collection) SUPERSET.get(rightName);

       return rightSuperSet.stream().flatMap(x->leftProperSubset.stream().filter(s->x.getValue3().equals(s.getValue0()))).findAny().isPresent();

    }

    public static Sextet getMatchMaxCardinalityTuple2 (String leftName, String rightName){
        /**Use this for inherited relationships when name do not longer match */

        //System.err.println("DEBUG MAXXXXXXXXX "+leftname+" "+rightName);
        Sextet properSub= null;

        Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> leftProperSubset= (Collection) PROPERSUBSET.get(leftName);
        Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> rightProperSubset= (Collection) PROPERSUBSET.get(rightName);
        Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> rightSuperSet= (Collection) SUPERSET.get(rightName);
        boolean isInProperSubsetSuperset= false;
        final Comparator<Sextet<String, Set<String>, Long, String, Set<String>, Long>> comp = (s1, s2) -> Long.compare(s1.getValue2(), s2.getValue2());

        if(rightSuperSet!=null && leftProperSubset!=null) {
            //this didn't work
           // isInProperSubsetSuperset = rightSuperSet.stream().anyMatch(s -> s.getValue0().equals(leftProperSubset.iterator().next().getValue3())&& !s.getValue1().isEmpty());
            isInProperSubsetSuperset = isInProperSubsetSuperset(leftName,rightName);

            if(isInProperSubsetSuperset){
                 properSub = leftProperSubset.stream().filter(s-> s.getValue3().equals(rightSuperSet.iterator().next().getValue0())).max(comp).get();
              //  properSub = leftProperSubset.stream().flatMap(x->leftProperSubset.stream().filter(s->x.getValue3().equals(s.getValue0()))).max(comp).get();
                //System.out.println(leftname+rightName+" Hello");

            }
        }
        /*if(!rightProperSubset.stream().anyMatch(s-> s.getValue3().equals(leftProperSubset.iterator().next().getValue0())))
        {
            rightProperSubset=(Collection) superset.get(rightName);
        }*/
        //TODO add !isInProperSubset check the if it is in superset  ?

      else  if(leftProperSubset!=null && rightProperSubset!=null  ) {

            try {
                // properSub = leftProperSubset.stream().filter(s -> s.getValue3().equals(rightName)).max(comp).get();
                //properSub = rightProperSubset.stream().filter(s-> s.getValue3().equals(leftProperSubset.iterator().next().getValue0())).max(comp).get();
                properSub = leftProperSubset.stream().filter(s-> s.getValue3().equals(rightProperSubset.iterator().next().getValue0())).max(comp).get();

            } catch (NoSuchElementException e) {
                properSub = leftProperSubset.stream().max(comp).get();
                //TODO handle NoSucheElementException for Claim Payment1&Claim
                // System.err.println("StackTrace"+leftname+" "+rightName);
                //System.err.println("ProperSub"+properSub.toString());
                //e.printStackTrace();
                return properSub;

            }
        }
        //System.out.println("GET MAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
        //System.out.println(leftProperSubset.stream().max(comp).get().toString());
        return properSub;
    }

   /** public static Sextet getMaxCardinalityTuple (String leftname, String rightName){

        System.err.println("DEBUG MAXXXXXXXXX "+leftname+" "+rightName);


        Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> leftProperSubset= (Collection) properSubset.get(leftname);
        final Comparator<Sextet<String,Set<String>,Long,String,Set<String>,Long>> comp = (s1,s2) -> Long.compare(s1.getValue2(),s2.getValue2());
        Sextet properSub =leftProperSubset.stream().filter(s->s.getValue3().equals(rightName)).max(comp).get();


        System.out.println("GET MAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
        System.out.println(leftProperSubset.stream().filter(s->s.getValue3().equals(rightName)).max(comp).get().toString());

        return properSub;



    }**/

    public static boolean checkMatchProperSubset (String lname , String rname )
    {
        Boolean checked = false;

        if (isMatchProperSubset2(lname,rname)){


            if(!hasBeenJoined2(lname,rname)&&!hasAncestors(lname)) {
                //  System.out.println("************************************");
                //System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
                //System.out.println("GROUP by is proper subset on the lefft" + lname + " on the right" + rname);

                Sextet<String,Set<String>,Long,String,Set<String>,Long>  temp = MetadataService.getMatchMaxCardinalityTuple2(lname, rname);
                boolean debug1 = SEPTET.containsValue(temp);
                boolean debug2 = hasDescendants(rname);
                boolean debug3 = isPartOfRegroup(lname);
                boolean debug4 = isPartOfRegroup2(lname);


                //mark tuple as used
                if(!SEPTET.containsValue(temp)&&!hasDescendants(rname)&&!isPartOfRegroup(rname)&&!isPartOfRegroup2(lname)) {

                     System.out.println(rname);
                    // System.out.println("hasDescendants "+hasDescendants(rname));
                    SEPTET.put(lname, temp);
                    Triplet<String, String, Boolean> transformed = Triplet.with(lname, rname, true);
                    JOINTRIPLET.put(lname,transformed);
                    // System.out.println("Regroup using this tuple");
                    // System.out.println("lname"+lname);
                    // System.out.println(temp.toString());
                    String newName = lname+"1";
                    JOINTRIPLET.put(newName,transformed);
                    System.out.println("###########################");
                    System.out.println("Create new leftname "+newName);

                    /*************METADATA STUFF***************/
                    /***Add transform start and end      */
                    TRANSFORMSTARTEND.put(lname,newName);
                    TRANSFORMSTARTEND2.put(newName,lname);
                    //JOINORREGROUPTRIPLET.put(lname,transformed);
                    JOINORREGROUPTRIPLET.put(newName,transformed);

                    /*******************************************/

                    HLLDB.TEMPREGROUPED.add(newName);
                    HLLDB.IDENTIFIEDBY.put(newName,temp.getValue1().toString());
                    //TODO new stuff identifiedBy2
                    Set<String> tmpSet = new HashSet<>();
                    tmpSet.add(temp.getValue1().toString());
                    IDENTIFIEDBY2.put(newName,tmpSet);
                    //Sextet<String,Set<String>,Long,String,Set<String>,Long> tempSextet2  =
                    //      Sextet.with(newName,temp.getValue1(),temp.getValue2(),temp.getValue3(),temp.getValue4(),temp.getValue5());

                    checked = true;

                    //System.out.println("New Sextet "+tempSextet2);
                    // subset.put(newName,tempSextet2);
                    SUBSET.put(newName,temp);
                    if (isSupersetEmpty(rname))
                    {
                        /**prepering the second part of the relation*/
                        /**meaning the data sets have common properSubsets and need to be regrouped **/
                        Sextet<String,Set<String>,Long,String,Set<String>,Long>  temp2 = MetadataService.getMatchMaxCardinalityTuple2(rname, lname);
                        SEPTET.put(rname,temp2);
                        Triplet<String, String, Boolean> transformedR = Triplet.with(rname, lname, true);
                        JOINTRIPLET.put(rname,transformedR);
                        String newNameR = rname+"1";
                        System.out.println("###########################");
                        System.out.println("Create new rname "+newNameR);
                        IDENTIFIEDBY.put(newNameR,temp2.getValue1());
                        TEMPREGROUPED.add(newNameR);
                        //TODO new stuff
                        Set<String> tmpSet2 = new HashSet<>();
                        tmpSet2.add(temp2.getValue1().toString());
                        IDENTIFIEDBY2.put(newNameR,tmpSet2);

                        /*************METADATA STUFF***************/
                        /***Add transform start and end      */
                        TRANSFORMSTARTEND.put(rname,newNameR);
                        TRANSFORMSTARTEND2.put(newNameR,rname);
                        JOINORREGROUPTRIPLET.put(newNameR,transformedR);

                        SUPERSET.put(newNameR,temp2);
                    }
                    /***METADATA*******/
                    //JOINORREGROUPTRIPLET.put(newName,transformed);
                }

                //SUBSET.put(lname,temp);

                //TODONE copy properties to new instance
                // TODONE and mark grouping touple as used id put it in SEPTET



            }

            //properSubset.get(lname)
        }
        return checked;
    }

    public static boolean isSupersetEmpty (String rname)
    {
        Boolean yes = false;
        if(SUPERSET.get(rname)==null)
        {
            yes=true;
        }
        return yes;
    }
    public static boolean isMatchProperSubset2(String leftName, String rightName)
    {
        Boolean contains = false;
        Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> rightList= (Collection) SUPERSET.get(rightName);
        Sextet<String,Set<String>,Long,String,Set<String>,Long> temp = MetadataService.getMatchMaxCardinalityTuple2(leftName,rightName);
        // Collection<Sextet> properSubSets = (Collection) properSubset.get(leftName);
        Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> propSub= (Collection) PROPERSUBSET.get(leftName);


        if(temp!= null)
        {
            /**if less than the other than properSubset **/
            if(temp.getValue3().equals(rightName)&&(temp.getValue2().intValue() <= temp.getValue5().intValue())&& !temp.getValue1().isEmpty()) {

                //   System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%GROUP BY ON THIS COLUMN");
                //   System.out.println(temp.toString());


                contains = true;
            }
            else if (temp.getValue2().intValue() <= (temp.getValue5().intValue())&& !temp.getValue1().isEmpty())
            {
                contains=true;
            }
            /***this condition will always be true as since temp/getMatchMaxCardinality returns value from propersubset**/
            else if(propSub!=null && propSub.contains(temp)) {
                //now we check if right reverse is present in the SUPERSET

                if(rightList!=null)
                {
                    contains= rightList.stream().anyMatch(s -> s.getValue0().equals(temp.getValue3()) && s.getValue1().equals(temp.getValue4()));
                }

            }


        }
        return contains;
    }

    public static boolean hasBeenJoined2 (String leftName, String rightName)
    {
        Boolean hasBeenJoined = false;

        Triplet<String,String,Boolean> triplet = JOINTRIPLET.get(leftName);
        if (triplet!=null) {
            if ((triplet.getValue0() == leftName || triplet.getValue1() == leftName) && triplet.getValue2().equals(false)) {
                hasBeenJoined = true;
            }
        }
        //check on the right side
        // System.out.println("Checking riight Side "+rightName);
        Triplet<String,String,Boolean> rightTrip = JOINTRIPLET.get(rightName);
        if(rightTrip!= null)
        {
            if((rightTrip.getValue0() == rightName || rightTrip.getValue1()==rightName) && rightTrip.getValue2().equals(false) )
            {
                hasBeenJoined=true;
            }
        }

        return hasBeenJoined;
    }
    public static boolean hasAncestors(String leftName){
        Boolean ancestors = false;
        Collection<Sextet> col = (Collection) SUPERSET.get(leftName);
        if (col!=null){
            ancestors=true;
        }
        return ancestors;
    }

    public static boolean isPartOfRegroup (String name)
    {
        Boolean regroup = false;
        ArrayList<Boolean> isRegroup = new ArrayList<>();

        JOINTRIPLET.values().forEach((e)->{
            if(e.getValue1().equals(name) && e.getValue2().equals(true))
            {
                isRegroup.add(true);
            }
        });
        if(isRegroup.size()>0)
        {
            regroup=isRegroup.get(0);
        }
        if (isRegroup.size()>1)
        {
            System.err.println("Regroup contians multiple values");
        }
        return regroup;
    }

    public static boolean isPartOfRegroup2 (String name)
    {
        Boolean regroup = false;
        ArrayList<Boolean> isRegroup = new ArrayList<>();

        JOINTRIPLET.values().forEach((e)->{
            if(e.getValue0().equals(name) && e.getValue2().equals(true))
            {
                isRegroup.add(true);
            }
        });
        if(isRegroup.size()>0)
        {
            regroup=isRegroup.get(0);
        }
        if (isRegroup.size()>1)
        {
            System.err.println("Regroup2 contians multiple values");
        }
        return regroup;
    }
    public static boolean hasDescendants (String name){
        Boolean hasDescendants = false;



        hasDescendants= JOINTRIPLET.keySet().contains(name);


        return hasDescendants;


    }

    public static boolean isSubset2 (String leftName, String rightName) {
        Boolean contains = false;
        Collection<Sextet> col = (Collection) SUBSET.get(leftName);
        Collection<Sextet> colright = (Collection) SUPERSET.get(rightName);

        if (col != null) {
            if(colright != null) {
                // col.stream().filter(s -> s.getValue3().equals(col.iterator().next().getValue0()));
                contains = col.stream().anyMatch(s -> s.getValue3().equals(colright.iterator().next().getValue0()));
            }
        }
        return contains;
    }

    /**
     * determine whether two numbers are "approximately equal" by seeing if they
     * are within a certain "tolerance percentage," with `tolerancePercentage` given
     * as a percentage (such as 10.0 meaning "10%").
     *
     * @param tolerancePercentage 1 = 1%, 2.5 = 2.5%, etc.
     */
    public static boolean approximatelyEqual(float desiredValue, float actualValue, float tolerancePercentage)
    {
        float diff = Math.abs(desiredValue-actualValue);
        // System.out.println(diff);
        float tolerance = tolerancePercentage/100*desiredValue;
        // System.out.println(tolerance);
        return diff<tolerance;
    }

}
