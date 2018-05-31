package com.nereus;

import org.apache.commons.collections4.MultiMap;
import org.javatuples.Sextet;
import org.javatuples.Triplet;

import java.util.*;
import java.util.stream.Collectors;

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
        HashMap<Integer,Boolean> isInProperSubsetSuperset = new HashMap<>();
        isInProperSubsetSuperset.put(1,false);


        leftProperSubset.forEach((e)->{

            rightSuperSet.forEach((g)->{
                if(e.getValue3().equals(g.getValue0()))
                {
                    isInProperSubsetSuperset.put(1,true);
                }
            });
        });
           // ADDRESS was wrongly classified as superset of POLICY condition was true but could then not find maxCardTuple
           // returning null pointer exception      01/05/2018
           return leftProperSubset.stream().flatMap(x-> rightSuperSet.stream().filter(s->x.getValue3().equals(s.getValue0()))).findAny().isPresent();
             // rightSuperset contain policy-> address tuple but it was to subset. so need to qurey propersubset if it contains reference to superset
            // return rightSuperSet.stream().flatMap(x->leftProperSubset.stream().filter(s->x.getValue3().equals(s.getValue0()))).findAny().isPresent();

       // return isInProperSubsetSuperset.get(1);

    }

    public static Sextet getMatchMaxCardinalityTuple2 (String leftName, String rightName){
        /**Use this for inherited relationships when name do not longer match */

        //System.err.println("DEBUG MAXXXXXXXXX "+leftname+" "+rightName);
        Sextet properSub= null;

        Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> leftProperSubset= (Collection) PROPERSUBSET.get(leftName);
        Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> rightProperSubset= (Collection) PROPERSUBSET.get(rightName);
        Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> rightSuperSet= (Collection) SUPERSET.get(rightName);
        //getMatchBkFkTuple(leftName,rightName);

        boolean isInProperSubsetSuperset= false;
        final Comparator<Sextet<String, Set<String>, Long, String, Set<String>, Long>> comp = (s1, s2) -> Long.compare(s1.getValue2(), s2.getValue2());

        if(rightSuperSet!=null && leftProperSubset!=null) {
            //this didn't work
           // isInProperSubsetSuperset = rightSuperSet.stream().anyMatch(s -> s.getValue0().equals(leftProperSubset.iterator().next().getValue3())&& !s.getValue1().isEmpty());
            isInProperSubsetSuperset = isInProperSubsetSuperset(leftName,rightName);
         //   System.out.println(leftName+rightName+" HelloPrev");
            if(isInProperSubsetSuperset){
               // System.out.println(leftName+rightName+" Hello");
                 properSub = leftProperSubset.stream().filter(s-> s.getValue3().equals(rightSuperSet.iterator().next().getValue0())).max(comp).get();

              //  properSub = leftProperSubset.stream().flatMap(x->leftProperSubset.stream().filter(s->x.getValue3().equals(s.getValue0()))).max(comp).get();
                //


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
                //17/05
                //return properSub;

            }
        }
        //System.out.println("GET MAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
        //System.out.println(leftProperSubset.stream().max(comp).get().toString());
        return properSub;
    }

    /**
     * Takes collection of business key - foreign key relations tuples and
     * returns the matching one.
     * @param leftCollection
     * @param rightCollection
     * @return
     */

    private static Sextet getBkFkTuple(Collection leftCollection, Collection rightCollection)
    {
        Sextet bkFkTuple = null;
        Sextet bkFkTuple2 = null;
        Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> left = leftCollection;
        Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> right = rightCollection;
        /**
        Iterator iterOuter = leftCollection.iterator();

        while (iterOuter.hasNext()) {
            Sextet tmp = (Sextet) iterOuter.next();
            Iterator iterInner = rightCollection.iterator();
            while (iterInner.hasNext()) {
                Sextet innerTmp = (Sextet) iterInner.next();
                if (tmp.getValue0().equals(innerTmp.getValue3())
                        && tmp.getValue1().equals(innerTmp.getValue4())
                        && tmp.getValue3().equals(innerTmp.getValue0())
                        && tmp.getValue4().equals(innerTmp.getValue1())
                        ) {
                    bkFkTuple = tmp;

                }
            }


        }*/
         if(left!=null && right !=null)
         {
             bkFkTuple2 = left.stream().filter(l -> right.stream()
                     .anyMatch(r -> l.getValue0().equals(r.getValue3())
                             && l.getValue1().equals(r.getValue4())
                             && l.getValue3().equals(r.getValue0())
                             && l.getValue4().equals(r.getValue1())))
                     .findFirst()
                     .orElse(null);
         }


        return bkFkTuple2;
    }
    public static Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>>  getProperSubset( String dataSetName)
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

    public static Sextet getMatchBkFkTuple (String leftName, String rightName){
        /**Use this for inherited relationships when name do not longer match */

        Sextet properSub= null;

        //Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> rightProperSubCol = null;
       // Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> properSubCol = null;
       // Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> rightSuperSetCol = null;

        //Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> leftProperSubset= (Collection) PROPERSUBSET.get(leftName);
        //Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> rightProperSubset= (Collection) PROPERSUBSET.get(rightName);
        //Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> rightSuperSet= (Collection) SUPERSET.get(rightName);
        boolean isInProperSubsetSuperset= false;
        final Comparator<Sextet<String, Set<String>, Long, String, Set<String>, Long>> comp = (s1, s2) -> Long.compare(s1.getValue2(), s2.getValue2());

        if(getSuperset(rightName)!=null && getProperSubset(leftName)!=null) {

            isInProperSubsetSuperset = isInProperSubsetSuperset(leftName,rightName);

            if(isInProperSubsetSuperset){
           // if(true){ TODO://seems that condition does nothing above
             Collection   properSubCol = getProperSubset(leftName).stream().filter(e->{return isBkFkRelationTuple(e);}).collect(Collectors.toList());
            Collection rightSuperSetCol= getSuperset(rightName).stream().filter(e->{return isBkFkRelationTuple(e);}).collect(Collectors.toList());

                //if(properSubCol!=null && rightSuperSetCol!=null) {
                    properSub= getBkFkTuple(properSubCol,rightSuperSetCol);
               // }

            }
        }

        //TODO add !isInProperSubset check the if it is in superset  ?

        else  if(getProperSubset(leftName)!=null && getProperSubset(rightName)!=null  ) {

         //   try {
                // properSub = leftProperSubset.stream().filter(s -> s.getValue3().equals(rightName)).max(comp).get();
                //properSub = rightProperSubset.stream().filter(s-> s.getValue3().equals(leftProperSubset.iterator().next().getValue0())).max(comp).get();
                Collection properSubCol = getProperSubset(leftName).stream().filter(e->{return isBkFkRelationTuple(e);}).collect(Collectors.toList());

               // rightProperSubset.stream().allMatch(e->{return isBkFkRelationTuple(e);});
               Collection rightProperSubCol = getProperSubset(rightName).stream().filter(e->{return isBkFkRelationTuple(e);}).collect(Collectors.toList());
               // if(properSubCol!=null && rightProperSubCol!=null) {
                   properSub=getBkFkTuple(properSubCol,rightProperSubCol);

                //}



           // } catch (NoSuchElementException e) {
              //  properSub = leftProperSubset.stream().max(comp).get();
                //TODO handle NoSucheElementException for Claim Payment1&Claim
                // System.err.println("StackTrace"+leftname+" "+rightName);
                //System.err.println("ProperSub"+properSub.toString());
               // e.printStackTrace();
                //17/05
                //return properSub;

            //}
        }
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


            if(!hasBeenJoined2(lname,rname)
                    &&!hasAncestors(lname)
                   // && isBkFkRelation(lname,rname)
                    ) {
                //  System.out.println("************************************");
                //System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
                //System.out.println("GROUP by is proper subset on the lefft" + lname + " on the right" + rname);

                Sextet<String, Set<String>, Long, String, Set<String>, Long> temp = MetadataService.getMatchMaxCardinalityTuple2(lname, rname);
               Sextet<String, Set<String>, Long, String, Set<String>, Long> tempBkFk = MetadataService.getMatchBkFkTuple(lname,rname);
                boolean debug1 = SEPTET.containsValue(temp);
                boolean debug2 = hasDescendants(rname);
                boolean debug3 = isPartOfRegroup(lname);
                boolean debug4 = isPartOfRegroup2(lname);
                boolean debug5 = hasOtherBkFkRelations(lname, rname);
                boolean debug6 = isBkFkRelation(lname, rname);
                boolean debug7 = isBkFkRelationTuple(temp);
                System.out.print("%%%%%%%%@@@@@");
                System.out.println(" Is BKFK "+debug7);
                System.out.println(temp);
                System.out.println(tempBkFk);
                System.out.println("lname"+lname);
                System.out.println("rname"+rname);
                System.out.println("temp equeals tempBk"+temp.equals(tempBkFk));
                //looks like it works
                //TODO: test it on AVRO 5
               /**if(tempBkFk!=null) {
                    temp = tempBkFk;
                }**/
                //mark tuple as used
                //07/05
                // if(!SEPTET.containsValue(temp)&&!hasDescendants(rname)&&!isPartOfRegroup(rname)&&!isPartOfRegroup2(lname)) {
                //TODO: Add logic that verfies if isBKFkRelations and test on GHI.
                //TODO: this does not work in current state it works on GHI but not on lname.equals("STATPEJ.POC_CLAIMPAYMENT51AndSTATPEJ.POC_CLAIM51")  and STATPEJ.POC_POLICY
             if(
                   //  isBkFkRelation(lname,rname)
                     //        || isBkFkRelationTuple(temp)
                     true
                     ){

                if (!SEPTET.containsValue(temp)
                        && !hasDescendants(rname)
                        && !isPartOfRegroup(rname)
                        && !isPartOfRegroup2(lname)
                        && !hasOtherBkFkRelations(lname, rname)
                    //  && isBkFkRelation(lname,rname)
                    // &&isBkFkRelationTuple(temp)
                        ) {
                    System.out.println("is TmpBk null? "+tempBkFk);
                    System.out.println(temp);
                    //TODO:check order of joins and modify to bk-fk instead of max-cardinality
                    TMPJOINTUPLES.put(lname,temp);
                    boolean debug8 = isBkFkRelationTuple(temp);
                    System.out.println(rname);
                    // System.out.println("hasDescendants "+hasDescendants(rname));
                    SEPTET.put(lname, temp);
                    Triplet<String, String, Boolean> transformed = Triplet.with(lname, rname, true);
                    JOINTRIPLET.put(lname, transformed);
                    // System.out.println("Regroup using this tuple");
                    // System.out.println("lname"+lname);
                    // System.out.println(temp.toString());
                    String newName = lname + "1";
                    JOINTRIPLET.put(newName, transformed);
                    System.out.println("###########################");
                    System.out.println("Create new leftname " + newName);

                    /*************METADATA STUFF***************/
                    /***Add transform start and end      */
                    TRANSFORMSTARTEND.put(lname, newName);
                    TRANSFORMSTARTEND2.put(newName, lname);
                    //JOINORREGROUPTRIPLET.put(lname,transformed);
                    JOINORREGROUPTRIPLET.put(newName, transformed);

                    /*******************************************/

                    HLLDB.TEMPREGROUPED.add(newName);
                    HLLDB.IDENTIFIEDBY.put(newName, temp.getValue1().toString());
                    //TODO new stuff identifiedBy2
                    Set<String> tmpSet = new HashSet<>();
                    tmpSet.add(temp.getValue1().toString());
                    IDENTIFIEDBY2.put(newName, tmpSet);
                    //Sextet<String,Set<String>,Long,String,Set<String>,Long> tempSextet2  =
                    //      Sextet.with(newName,temp.getValue1(),temp.getValue2(),temp.getValue3(),temp.getValue4(),temp.getValue5());

                    checked = true;

                    //System.out.println("New Sextet "+tempSextet2);
                    // subset.put(newName,tempSextet2);
                    SUBSET.put(newName, temp);
                    if (isSupersetEmpty(rname)) {
                        /**prepering the second part of the relation*/
                        /**meaning the data sets have common properSubsets and need to be regrouped **/
                        Sextet<String, Set<String>, Long, String, Set<String>, Long> temp2 = MetadataService.getMatchMaxCardinalityTuple2(rname, lname);
                        SEPTET.put(rname, temp2);
                        Triplet<String, String, Boolean> transformedR = Triplet.with(rname, lname, true);
                        JOINTRIPLET.put(rname, transformedR);
                        String newNameR = rname + "1";
                        System.out.println("###########################");
                        System.out.println("Create new rname ln 328 MetadataService " + newNameR);
                        IDENTIFIEDBY.put(newNameR, temp2.getValue1());
                        TEMPREGROUPED.add(newNameR);
                        //TODO new stuff
                        Set<String> tmpSet2 = new HashSet<>();
                        tmpSet2.add(temp2.getValue1().toString());
                        IDENTIFIEDBY2.put(newNameR, tmpSet2);

                        /*************METADATA STUFF***************/
                        /***Add transform start and end      */
                        TRANSFORMSTARTEND.put(rname, newNameR);
                        TRANSFORMSTARTEND2.put(newNameR, rname);
                        JOINORREGROUPTRIPLET.put(newNameR, transformedR);

                        SUPERSET.put(newNameR, temp2);

                        //new 07/05
                        ProcessorService.transferRelations(lname, rname, newNameR);
                    }
                    /***METADATA*******/
                    //JOINORREGROUPTRIPLET.put(newName,transformed);
                }
            }
                //SUBSET.put(lname,temp);

                //TODONE copy properties to new instance
                // TODONE and mark grouping touple as used id put it in SEPTET



            }

            //properSubset.get(lname)
        }
        return checked;
    }
    public static boolean checkMatchProperSubsetWithDictionary (String lname , String rname )
    {
        Boolean checked = false;

        if (isMatchProperSubset2(lname,rname)){


            if(!hasBeenJoined2(lname,rname)
                    &&!hasAncestors(lname)
                // && isBkFkRelation(lname,rname)
                    ) {
                //  System.out.println("************************************");
                //System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
                //System.out.println("GROUP by is proper subset on the lefft" + lname + " on the right" + rname);

                Sextet<String, Set<String>, Long, String, Set<String>, Long> temp = MetadataService.getMatchMaxCardinalityTuple2(lname, rname);
                Sextet<String, Set<String>, Long, String, Set<String>, Long> tempBkFk = MetadataService.getMatchBkFkTuple(lname,rname);
                boolean debug1 = SEPTET.containsValue(temp);
                boolean debug2 = hasDescendants(rname);
                boolean debug3 = isPartOfRegroup(lname);
                boolean debug4 = isPartOfRegroup2(lname);
                boolean debug5 = hasOtherBkFkRelationsWithDictionary(lname, rname);
                boolean debug6 = isBkFkRelation(lname, rname);
                boolean debug7 = isBkFkRelationTuple(temp);
                System.out.print("%%%%%%%%@@@@@");
                System.out.println(" Is BKFK "+debug7);
                System.out.println(temp);
                System.out.println(tempBkFk);
                System.out.println("lname"+lname);
                System.out.println("rname"+rname);
                System.out.println("temp equeals tempBk"+temp.equals(tempBkFk));
                //looks like it works
                //TODO: test it on AVRO 5
                 if(tempBkFk!=null) {
                 temp = tempBkFk;
                 }
                //mark tuple as used
                //07/05
                // if(!SEPTET.containsValue(temp)&&!hasDescendants(rname)&&!isPartOfRegroup(rname)&&!isPartOfRegroup2(lname)) {
                //TODO: Add logic that verfies if isBKFkRelations and test on GHI.
                //TODO: this does not work in current state it works on GHI but not on lname.equals("STATPEJ.POC_CLAIMPAYMENT51AndSTATPEJ.POC_CLAIM51")  and STATPEJ.POC_POLICY
                if(
                        //isBkFkRelation(lname,rname)
                          //      || isBkFkRelationTuple(temp)
                        tempBkFk!=null
                        ){

                    if (!SEPTET.containsValue(temp)
                            && !hasDescendants(rname)
                            && !isPartOfRegroup(rname)
                            && !isPartOfRegroup2(lname)
                            && !hasOtherBkFkRelationsWithDictionary(lname, rname)
                        //  && isBkFkRelation(lname,rname)
                        // &&isBkFkRelationTuple(temp)
                            ) {
                        System.out.println("is TmpBk null? "+tempBkFk);
                        System.out.println(temp);
                        //TODO:check order of joins and modify to bk-fk instead of max-cardinality
                        TMPJOINTUPLES.put(lname,temp);
                        boolean debug8 = isBkFkRelationTuple(temp);
                        System.out.println(rname);
                        // System.out.println("hasDescendants "+hasDescendants(rname));
                        SEPTET.put(lname, temp);
                        Triplet<String, String, Boolean> transformed = Triplet.with(lname, rname, true);
                        JOINTRIPLET.put(lname, transformed);
                        // System.out.println("Regroup using this tuple");
                        // System.out.println("lname"+lname);
                        // System.out.println(temp.toString());
                        String newName = lname + "1";
                        JOINTRIPLET.put(newName, transformed);
                        System.out.println("###########################");
                        System.out.println("Create new leftname ln 435 MetadataService " + newName);

                        /*************METADATA STUFF***************/
                        /***Add transform start and end      */
                        TRANSFORMSTARTEND.put(lname, newName);
                        TRANSFORMSTARTEND2.put(newName, lname);
                        //JOINORREGROUPTRIPLET.put(lname,transformed);
                        JOINORREGROUPTRIPLET.put(newName, transformed);

                        /*******************************************/

                        HLLDB.TEMPREGROUPED.add(newName);
                        HLLDB.IDENTIFIEDBY.put(newName, temp.getValue1().toString());
                        //TODO new stuff identifiedBy2
                        Set<String> tmpSet = new HashSet<>();
                        tmpSet.add(temp.getValue1().toString());
                        IDENTIFIEDBY2.put(newName, tmpSet);
                        //Sextet<String,Set<String>,Long,String,Set<String>,Long> tempSextet2  =
                        //      Sextet.with(newName,temp.getValue1(),temp.getValue2(),temp.getValue3(),temp.getValue4(),temp.getValue5());

                        checked = true;

                        //System.out.println("New Sextet "+tempSextet2);
                        // subset.put(newName,tempSextet2);
                        SUBSET.put(newName, temp);
                        if (isSupersetEmpty(rname)) {
                            /**prepering the second part of the relation*/
                            /**meaning the data sets have common properSubsets and need to be regrouped **/
                            //Sextet<String, Set<String>, Long, String, Set<String>, Long> temp2 = MetadataService.getMatchMaxCardinalityTuple2(rname, lname);
                            Sextet<String, Set<String>, Long, String, Set<String>, Long> temp2 = MetadataService.getMatchBkFkTuple(rname, lname);
                            SEPTET.put(rname, temp2);
                            Triplet<String, String, Boolean> transformedR = Triplet.with(rname, lname, true);
                            JOINTRIPLET.put(rname, transformedR);
                            String newNameR = rname + "1";
                            System.out.println("###########################");
                            System.out.println("Create new rname " + newNameR);
                            IDENTIFIEDBY.put(newNameR, temp2.getValue1());
                            TEMPREGROUPED.add(newNameR);
                            //TODO new stuff
                            Set<String> tmpSet2 = new HashSet<>();
                            tmpSet2.add(temp2.getValue1().toString());
                            IDENTIFIEDBY2.put(newNameR, tmpSet2);

                            /*************METADATA STUFF***************/
                            /***Add transform start and end      */
                            TRANSFORMSTARTEND.put(rname, newNameR);
                            TRANSFORMSTARTEND2.put(newNameR, rname);
                            JOINORREGROUPTRIPLET.put(newNameR, transformedR);

                            SUPERSET.put(newNameR, temp2);

                            //new 07/05
                            ProcessorService.transferRelations(lname, rname, newNameR);
                        }
                        /***METADATA*******/
                        //JOINORREGROUPTRIPLET.put(newName,transformed);
                    }
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
      //  Sextet<String,Set<String>,Long,String,Set<String>,Long> temp = MetadataService.getMatchMaxCardinalityTuple2(leftName,rightName);
        Sextet<String,Set<String>,Long,String,Set<String>,Long> temp = MetadataService.getMatchBkFkTuple(leftName,rightName);
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

    /**
     * Check if the datasets have business key, foreign key relation in propersubset
     * @param leftName
     * @param rightName
     * @return boolean
     */
    public static boolean isBkFkRelation(String leftName, String rightName)
    {
        Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> col =  (Collection) PROPERSUBSET.get(leftName);
        //TODO: needs to be changed to something like PROPERSUBSET.get(rightName) and compare these twe
        //TODO: something like getMatchMaxCardinalityTuple2

        boolean debug = col.stream().anyMatch((e)->isBusinessKey2(e.getValue0(),e.getValue1()) );
        boolean debug2 = col.stream().anyMatch((e)->isBusinessKey2(e.getValue0(),e.getValue4()) );
         boolean debug3 = col.stream().anyMatch((e)->isBusinessKey2(e.getValue0(),e.getValue1()) && isBusinessKey2(e.getValue0(),e.getValue4()) && e.getValue3().equals(rightName));
        boolean debug4 = col.stream().anyMatch((e)->isBusinessKey2(e.getValue3(),e.getValue4()) && isBusinessKey2(e.getValue3(),e.getValue1()) && e.getValue3().equals(rightName));
          if (debug&debug2!=debug3)
          {
              System.err.println("*******************************");
              System.err.println("isBkFkRelation do not equal each other MetadataService ln 330");
              System.err.println("*******************************");
          }
        return debug3 || debug4;
    }

    /**
     * Check if suggested tuple is business key / foreign key relation
     * @param joinTuple
     * @return
     */

    public static boolean isBkFkRelationTuple (Sextet<String,Set<String>,Long,String,Set<String>,Long> joinTuple)
    {
        /**
         * For a sextet " [Policy,[customer],9971,Customer,[customer],9971]"
         * check if it is BK-FK relation
         */
        //is BK on the left
        boolean debug = isBusinessKey2(joinTuple.getValue0(),joinTuple.getValue1())&&isBusinessKey2(joinTuple.getValue0(),joinTuple.getValue4());
        //is BK on the right
        boolean debug2 = isBusinessKey2(joinTuple.getValue3(),joinTuple.getValue4())&&isBusinessKey2(joinTuple.getValue3(),joinTuple.getValue1());
        return  debug || debug2;
    }
    public static boolean hasOtherBkFkRelations(String leftName, String rightName )
    {
        //Check if there is BK -FK relationshp to others than current data set
        // if yes wait
        boolean hasBkFkRelations = false;
        Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> col =  (Collection) PROPERSUBSET.get(leftName);
        boolean debug = isBkFkRelation(leftName,rightName);
        //Make sure DatasetNema(value0) and columnName(value1) is BK and is not related to other data sets(value3)
        // if true it has other relationships and we should wait

        /** GHI file In this case we look into Policy Customer
         * col looks like this A Policy,[customer],9971,Customer,[customer],9971]
         * B Policy,[policy],10060,Claim,[policy],5055]
         * C Policy,[customer],9971,Address,[customer],9971
         * Since we are considering Policy Customer FK-BK relation we would have to transform
         * but we also have Policy BK Claim FK relationship so in we case do nothing and move on
         * beacuse Claims does note equal Customer
         * Policy and Address relationship is ignored none of them is BK
         */
        hasBkFkRelations= col.stream().anyMatch((e)->isBusinessKey2(e.getValue0(),e.getValue1()) && !e.getValue3().equals(rightName) );
        // if (isBusinessKey2(leftName,column))
        //{

        //  hasBkFkRelations = col.stream().anyMatch((e)-> e.getValue4().equals(column) && !e.getValue3().equals(rightName));
        //.filter((e)->{e.getValue5().equals(column)}).findAny().isPresent();
        //}

        return hasBkFkRelations;
    }

    public static boolean hasOtherBkFkRelationsWithDictionary(String leftName, String rightName )
    {
        //Check if there is BK -FK relationshp to others than current data set
        // if yes wait
        boolean hasBkFkRelations = false;
        Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> col =  (Collection) PROPERSUBSET.get(leftName);
     //   Collection<Sextet<String,Set<String>,Long,String,Set<String>,Long>> col1 =  (Collection) PROPERSUBSET.get(leftName);
      /**  col=col.stream().filter(e->!SEPTET.containsValue(e.getValue0()) &&
                !SEPTET.containsValue(e.getValue1())
                && ! SEPTET.containsValue(e.getValue3())
                && ! SEPTET.containsValue(e.getValue4())
        ).collect(Collectors.toList());**/
        boolean debug = isBkFkRelation(leftName,rightName);

        //Make sure DatasetNema(value0) and columnName(value1) is BK and is not related to other data sets(value3)
        // if true it has other relationships and we should wait

        /** GHI file In this case we look into Policy Customer
         * col looks like this A Policy,[customer],9971,Customer,[customer],9971]
         * B Policy,[policy],10060,Claim,[policy],5055]
         * C Policy,[customer],9971,Address,[customer],9971
         * Since we are considering Policy Customer FK-BK relation we would have to transform
         * but we also have Policy BK Claim FK relationship so in we case do nothing and move on
         * beacuse Claims does note equal Customer
         * Policy and Address relationship is ignored none of them is BK
         */
        /** String rname= col.stream().filter(e->isBkFkRelationTuple(e))
                  .findFirst()
                  .map(e->e.getValue3());**/
        Sextet temp = col.stream().filter(e->isBkFkRelationTuple(e)).findFirst().get();
        String rname = temp.getValue3().toString();


        boolean debug7 = col.stream().anyMatch((e)-> isBkFkRelationTuple(e) && ! e.getValue3().equals(rname));
        hasBkFkRelations= col.stream().anyMatch((e)->isBusinessKey2(e.getValue0(),e.getValue1()) && !e.getValue3().equals(rightName) );
        if (debug7!=hasBkFkRelations)
        {
            System.err.println("Not equal "+leftName+" "+rightName);
        }
        // if (isBusinessKey2(leftName,column))
        //{

        //  hasBkFkRelations = col.stream().anyMatch((e)-> e.getValue4().equals(column) && !e.getValue3().equals(rightName));
        //.filter((e)->{e.getValue5().equals(column)}).findAny().isPresent();
        //}

       // return hasBkFkRelations;
        return debug7;
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

    /**
     * Initailzes SETCARDINALITY  map with source nodes
     * this map will hold information about carditanlity o
     * for each column in the data set.
     *
     */
    public static void initCardinalityMap()
    {
        SOURCE_NODES.forEach((e)->{
            HashMap<Set<String>,Integer> tmpMap = new HashMap<>();
            SETCARDINALITY.put(e,tmpMap);
        });
    }

    public static void hardCodeBusinessKeys()
    {
       String claimnumber = "claimnumber";
        Set<String> claimBk = new HashSet<>();
        claimBk.add(claimnumber);
        TEMPBKEYS.put(CLAIM_TOPIC,claimBk);
        String customer = "customer";
        Set<String> customerBk = new HashSet<>();
        customerBk.add(customer);
        TEMPBKEYS.put(CUSTOMER_TOPIC,customerBk);
        String payment = "claimnumber";
        Set<String> paymentBk = new HashSet<>();
        paymentBk.add(payment);
        TEMPBKEYS.put(PAYMENT_TOPIC,paymentBk);
        String policy = "policy";
        Set<String> policyBk = new HashSet<>();
        policyBk.add(policy);
        TEMPBKEYS.put(POLICY_TOPIC,policyBk);
        String address = "address";
        Set<String> addressBk = new HashSet<>();
        addressBk.add(address);
        TEMPBKEYS.put(ADDRESS_TOPIC,addressBk);





    }

    public static void hardCodeBusinessKeysAvro()
    {
       //Avro 5
        String claimnumber = "CLAIMID";
        Set<String> claimBk = new HashSet<>();
        claimBk.add(claimnumber);
        TEMPBKEYS.put(CLAIM_TOPIC,claimBk);
        String customer = "CUSTOMERID";
        Set<String> customerBk = new HashSet<>();
        customerBk.add(customer);
        TEMPBKEYS.put(CUSTOMER_TOPIC,customerBk);
        String payment = "PAYMENTID";
        Set<String> paymentBk = new HashSet<>();
        paymentBk.add(payment);
        TEMPBKEYS.put(PAYMENT_TOPIC,paymentBk);
        String policy = "POLICYID";
        Set<String> policyBk = new HashSet<>();
        policyBk.add(policy);
        TEMPBKEYS.put(POLICY_TOPIC,policyBk);
        String address = "ADDRESSID";
        Set<String> addressBk = new HashSet<>();
        addressBk.add(address);
        TEMPBKEYS.put(ADDRESS_TOPIC,addressBk);
        String claimhandler = "CLAIMHANDLERID";
        Set<String> claimHandlerBk = new HashSet<>();
        claimHandlerBk.add(claimhandler);
        TEMPBKEYS.put(CLAIMHANDLER_TOPIC,claimHandlerBk);



    }

    public static void hardCodeBusinessKeysAvroA2()
    {
        //Avro 5
        String claimnumber = "CLAIMID";
        Set<String> claimBk = new HashSet<>();
        claimBk.add(claimnumber);
        TEMPBKEYS.put(CLAIM_TOPIC,claimBk);
        String customer = "CUSTOMERID";
        Set<String> customerBk = new HashSet<>();
        customerBk.add(customer);
        TEMPBKEYS.put(CUSTOMER_TOPIC,customerBk);
        String payment = "PAYMENTID";
        Set<String> paymentBk = new HashSet<>();
        paymentBk.add(payment);
        TEMPBKEYS.put(PAYMENT_TOPIC,paymentBk);
        String policy = "POLICYID";
        Set<String> policyBk = new HashSet<>();
        policyBk.add(policy);
        TEMPBKEYS.put(POLICY_TOPIC,policyBk);
        String address = "ADDRESSID";
        Set<String> addressBk = new HashSet<>();
        addressBk.add(address);
        TEMPBKEYS.put(ADDRESS_TOPIC,addressBk);
        String claimhandler = "CLAIMHANDLERID";
        Set<String> claimHandlerBk = new HashSet<>();
        claimHandlerBk.add(claimhandler);
        TEMPBKEYS.put(CLAIMHANDLER_TOPIC,claimHandlerBk);
        String address2 = "ADDRESSID";
        Set<String> address2Bk = new HashSet<>();
        addressBk.add(address2);
        String ADDRESS2_TOPIC = "STATPEJ.POC_ADDRESS52";
        TEMPBKEYS.put(ADDRESS2_TOPIC,addressBk);



    }
    public  static boolean isBusinessKey2(String dataSetName, Set<String> columnName)
    {
        Set<String> dataSetBk = TEMPBKEYS.get(dataSetName);

        boolean debug1 = dataSetBk.equals(columnName);
        return dataSetBk.equals(columnName);
    }

    /**
     *  * Determine if column is a candidate key / business key deducing base on cardinality
     *      * Since we deal with transactions log PK will be probacly BK+timestamp
     *      * So we assume BK will have second highest cardinality
     * @param dataSetName
     * @param column
     * @return
     */

    public static boolean isBusinessKey(String dataSetName, Set<String> column )
    {
        //SetCArdinality for claim contains only one column?
       // HashMap<String,HashMap<Set<String>,Integer>> setCard = (HashMap<String,HashMap<Set<String>,Integer>>) SETCARDINALITY.clone();
        HashMap<Set<String>,Integer> columnMap = SETCARDINALITY.get(dataSetName);
        //Let's find primaryKey first
       Set<String> primaryKey= Collections.max(columnMap.entrySet(), Comparator.comparingInt(Map.Entry::getValue)).getKey();
       Integer primaryKeyCardinality =  Collections.max(columnMap.entrySet(), Comparator.comparingInt(Map.Entry::getValue)).getValue();
       //the second best should be BK so let's remove PK and get max again
       // columnMap.remove(primaryKey);
        Map<Set<String>,Integer> tempMap = columnMap.entrySet()
                .stream()
                .filter(map->!map.getKey().equals(primaryKey))
                .collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));

       /** columnMap.forEach((k,v)->{
            if(!k.equals(primaryKey))
            {
                tempMap.put(k,v);
            }
        });**/
        Set<String> businessKey = Collections.max(tempMap.entrySet(), Comparator.comparingInt(Map.Entry::getValue)).getKey();
        System.out.println(dataSetName+" BK "+ businessKey);

        return businessKey.equals(column);
    }


}
