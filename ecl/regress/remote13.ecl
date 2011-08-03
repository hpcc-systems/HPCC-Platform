/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#option ('targetClusterType', 'roxie');

import lib_stringlib,std.system.thorlib;
prefix := 'hthor';
useLayoutTrans := false;
useLocal := false;
usePayload := false;
useVarIndex := false;
useDynamic := false;
//define constants
DG_GenFlat           := true;   //TRUE gens FlatFile
DG_GenChild          := true;   //TRUE gens ChildFile
DG_GenGrandChild     := true;   //TRUE gens GrandChildFile
DG_GenIndex          := true;   //TRUE gens FlatFile AND the index
DG_GenCSV            := true;   //TRUE gens CSVFile
DG_GenXML            := true;   //TRUE gens XMLFile
DG_GenVar            := true;   //TRUE gens VarFile only IF MaxField >= 3

DG_MaxField          := 3;    // maximum number of fields to use building the data records
DG_MaxChildren       := 3;    //maximum (1 to n) number of child recs

                    // generates (#parents * DG_MaxChildren) records
DG_MaxGrandChildren  := 3;    //maximum (1 to n) number of grandchild recs
                    // generates (#children * DG_MaxGrandChildren) records

#if (useDynamic=true)
 VarString EmptyString := '' : STORED('dummy');
 filePrefix := prefix + EmptyString;
 #option ('allowVariableRoxieFilenames', 1);
#else
 filePrefix := prefix;
#end

DG_FileOut           := '~REGRESS::' + filePrefix + '::DG_';
DG_ParentFileOut     := '~REGRESS::' + filePrefix + '::DG_Parent.d00';
DG_ChildFileOut      := '~REGRESS::' + filePrefix + '::DG_Child.d00';
DG_GrandChildFileOut := '~REGRESS::' + filePrefix + '::DG_GrandChild.d00';
DG_FetchFileName     := '~REGRESS::' + filePrefix + '::DG_FetchFile';
DG_FetchFilePreloadName := '~REGRESS::' + filePrefix + '::DG_FetchFilePreload';
DG_FetchFilePreloadIndexedName := '~REGRESS::' + filePrefix + '::DG_FetchFilePreloadIndexed';
DG_FetchIndex1Name   := '~REGRESS::' + filePrefix + '::DG_FetchIndex1';
DG_FetchIndex2Name   := '~REGRESS::' + filePrefix + '::DG_FetchIndex2';
DG_FetchIndexDiffName:= '~REGRESS::' + filePrefix + '::DG_FetchIndexDiff';
DG_MemFileName       := '~REGRESS::' + filePrefix + '::DG_MemFile';

//record structures
DG_FetchRecord := RECORD
  INTEGER8 sequence;
  STRING2  State;
  STRING20 City;
  STRING25 Lname;
  STRING15 Fname;
END;

DG_FetchFile   := DATASET(DG_FetchFileName,{DG_FetchRecord,UNSIGNED8 __filepos {virtual(fileposition)}},FLAT);
DG_FetchFilePreload := PRELOAD(DATASET(DG_FetchFilePreloadName,{DG_FetchRecord,UNSIGNED8 __filepos {virtual(fileposition)}},FLAT));
DG_FetchFilePreloadIndexed := PRELOAD(DATASET(DG_FetchFilePreloadIndexedName,{DG_FetchRecord,UNSIGNED8 __filepos {virtual(fileposition)}},FLAT),1);

#IF (useLayoutTrans=false)
 #IF (usePayload=false)
  DG_FetchIndex1 := INDEX(DG_FetchFile,{Lname,Fname,__filepos},DG_FetchIndex1Name);
  DG_FetchIndex2 := INDEX(DG_FetchFile,{Lname,Fname, __filepos}, DG_FetchIndex2Name);
 #ELSE
  #IF (useVarIndex=true)
   DG_FetchIndex1 := INDEX(DG_FetchFile,{Lname,Fname},{STRING fn := TRIM(Fname), state, STRING100 x {blob}:= fname, __filepos},DG_FetchIndex1Name);
   DG_FetchIndex2 := INDEX(DG_FetchFile,{Lname,Fname},{STRING fn := TRIM(Fname), state, STRING100 x {blob}:= fname, __filepos},DG_FetchIndex2Name);
  #ELSE
   DG_FetchIndex1 := INDEX(DG_FetchFile,{Lname,Fname},{state ,__filepos},DG_FetchIndex1Name);
   DG_FetchIndex2 := INDEX(DG_FetchFile,{Lname,Fname},{state, __filepos}, DG_FetchIndex2Name);
  #END
 #END
#ELSE
 // Declare all indexes such that layout translation is required... Used at run-time only, not at setup time...
 #IF (usePayload=false)
  DG_FetchIndex1 := INDEX(DG_FetchFile,{Fname,Lname,__filepos},DG_FetchIndex1Name);
  DG_FetchIndex2 := INDEX(DG_FetchFile,{Fname,Lname, __filepos}, DG_FetchIndex2Name);
 #ELSE
  #IF (useVarIndex=true)
   DG_FetchIndex1 := INDEX(DG_FetchFile,{Fname,Lname},{STRING fn := TRIM(Fname), state, STRING100 x {blob}:= fname, __filepos},DG_FetchIndex1Name);
   DG_FetchIndex2 := INDEX(DG_FetchFile,{Fname,Lname},{STRING fn := TRIM(Fname), state, STRING100 x {blob}:= fname, __filepos},DG_FetchIndex2Name);
  #ELSE
   DG_FetchIndex1 := INDEX(DG_FetchFile,{Fname,Lname},{state ,__filepos},DG_FetchIndex1Name);
   DG_FetchIndex2 := INDEX(DG_FetchFile,{Fname,Lname},{state, __filepos}, DG_FetchIndex2Name);
  #END
 #END
#END
DG_OutRec := RECORD
    unsigned4  DG_ParentID;
    string10  DG_firstname;
    string10  DG_lastname;
    unsigned1 DG_Prange;
END;

DG_OutRecChild := RECORD
    unsigned4  DG_ParentID;
    unsigned4  DG_ChildID;
    string10  DG_firstname;
    string10  DG_lastname;
    unsigned1 DG_Prange;
END;

#if(DG_GenVar = TRUE)
DG_VarOutRec := RECORD
  DG_OutRec;
  IFBLOCK(self.DG_Prange%2=0)
    string20 ExtraField;
  END;
END;
#end

//DATASET declarations
DG_BlankSet := dataset([{0,'','',0}],DG_OutRec);

#if(DG_GenFlat = TRUE OR DG_GenIndex = TRUE)
DG_FlatFile      := DATASET(DG_FileOut+'FLAT',{DG_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
DG_FlatFileEvens := DATASET(DG_FileOut+'FLAT_EVENS',{DG_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
#end

#if(DG_GenIndex = TRUE)
DG_indexFile      := INDEX(DG_FlatFile,
    RECORD
#if(useLayoutTrans=false)
      DG_firstname;
      DG_lastname;
#else
      DG_lastname;
      DG_firstname;
#end
#if(usePayload = TRUE)
    END , RECORD
#end
      DG_Prange;
      filepos
    END,DG_FileOut+'INDEX');

DG_indexFileEvens := INDEX(DG_FlatFileEvens,
    RECORD
#if(useLayoutTrans=false)
      DG_firstname;
      DG_lastname;
#else
      DG_lastname;
      DG_firstname;
#end
#if(usePayload = TRUE)
    END , RECORD
#end
      DG_Prange;
      filepos
    END,DG_FileOut+'INDEX_EVENS');
#end

#if(DG_GenCSV = TRUE)
DG_CSVFile   := DATASET(DG_FileOut+'CSV',DG_OutRec,CSV);
#end

#if(DG_GenXML = TRUE)
DG_XMLFile   := DATASET(DG_FileOut+'XML',DG_OutRec,XML);
#end

#if(DG_GenVar = TRUE)
DG_VarOutRecPlus := RECORD
  DG_VarOutRec,
  unsigned8 __filepos { virtual(fileposition)};
END;

DG_VarFile   := DATASET(DG_FileOut+'VAR',DG_VarOutRecPlus,FLAT);
DG_VarIndex  := INDEX(DG_VarFile,{
#if(useLayoutTrans=false)
      DG_firstname;
      DG_lastname;
#else
      DG_lastname;
      DG_firstname;
#end
__filepos},DG_FileOut+'VARINDEX');
DG_VarVarIndex  := INDEX(DG_VarFile,{
#if(useLayoutTrans=false)
      DG_firstname;
      DG_lastname;
#else
      DG_lastname;
      DG_firstname;
#end
__filepos},{ string temp_blob1 := TRIM(ExtraField); string10000 temp_blob2 {blob} := ExtraField },DG_FileOut+'VARVARINDEX');
#end
#if(DG_GenChild = TRUE)
DG_ParentFile  := DATASET(DG_ParentFileOut,{DG_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
DG_ChildFile   := DATASET(DG_ChildFileOut,{DG_OutRecChild,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
  #if(DG_GenGrandChild = TRUE)
DG_GrandChildFile := DATASET(DG_GrandChildFileOut,{DG_OutRecChild,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
  #end
#end

// NOTE - uses sequential as otherwise we use too many threads (allegedly)

// Try some remote activities reading from normal indexes and local indexes

index_best := topn(DG_FetchIndex1(LName[1]='A'), 3, LName, FName);
index_best_all := allnodes(index_best);
o1 := output(sort(index_best_all, LName, FName), {LName, FName});

index_best_all_local := allnodes(LOCAL(index_best));
o2 := output(sort(index_best_all_local, LName, FName), {LName, FName});

// Now try with disk files

disk_best := topn(DG_FetchFile(LName[1]='A'), 3, LName, FName);
disk_best_all := allnodes(disk_best);
o3 := output(sort(disk_best_all, LName, FName), {LName, FName});

disk_best_all_local := allnodes(LOCAL(disk_best));
o4 := output(sort(disk_best_all_local, LName, FName), {LName, FName});

// Now try with in-memory files

preload_best := topn(DG_FetchFilePreload(LName[1]='A'), 3, LName, FName);
preload_best_all := allnodes(preload_best);
o5 := output(sort(preload_best_all, LName, FName), {LName, FName});

preload_best_all_local := allnodes(LOCAL(preload_best));
o6 := output(sort(preload_best_all_local, LName, FName), {LName, FName});

// Now try with keyed in-memory files

preload_indexed_best := topn(DG_FetchFilePreloadIndexed(KEYED(LName[1]='A')), 3, LName, FName);
preload_indexed_best_all := allnodes(preload_indexed_best);
o7 := output(sort(preload_indexed_best_all, LName, FName), {LName, FName});

preload_indexed_best_all_local := allnodes(LOCAL(preload_indexed_best));
o8 := output(sort(preload_indexed_best_all_local, LName, FName), {LName, FName});


SEQUENTIAL(o1,o2,o3,o4,o5,o6,o7,o8);
