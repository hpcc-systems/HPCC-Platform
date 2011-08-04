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

import lib_stringlib,std.system.thorlib;
prefix := 'hthor';
usePayload := false;
useVarIndex := false;
//define constants
DG_GenFlat           := true;   //TRUE gens FlatFile
DG_GenChild          := true;   //TRUE gens ChildFile
DG_GenGrandChild     := true;   //TRUE gens GrandChildFile
DG_GenIndex          := true;   //TRUE gens FlatFile AND the index
DG_GenCSV            := true;   //TRUE gens CSVFile
DG_GenXML            := true;   //TRUE gens XMLFile
DG_GenVar            := true;   //TRUE gens VarFile only IF MaxField >= 3

//total number of data records generated = DG_MaxElement raised to the DG_MaxField power
//                                         maximum is 4,294,967,296 recs -- 16 to the 8th power

DG_MaxElement        := 4;    //base     - maximum (1 to 16) number of set elements to use building the data records
DG_MaxField          := 3;    //exponent - maximum (1 to 8) number of fields to use building the data records
DG_MaxChildren       := 3;    //maximum (1 to n) number of child recs
                    // generates (#parents * DG_MaxChildren) records
DG_MaxGrandChildren  := 3;    //maximum (1 to n) number of grandchild recs
                    // generates (#children * DG_MaxGrandChildren) records


DG_FileOut           := '~REGRESS::' + prefix + '::DG_'+DG_MaxElement+'_'+DG_MaxField+'_';
DG_ParentFileOut     := '~REGRESS::' + prefix + '::DG_'+DG_MaxElement+'_'+DG_MaxField+'_'+'Parent'+'.d00';
DG_ChildFileOut      := '~REGRESS::' + prefix + '::DG_'+DG_MaxElement+'_'+DG_MaxField+'_'+'Child'+'.d00';
DG_GrandChildFileOut := '~REGRESS::' + prefix + '::DG_'+DG_MaxElement+'_'+DG_MaxField+'_'+'GrandChild'+'.d00';
DG_FetchFileName     := '~REGRESS::' + prefix + '::DG_FetchFile';
DG_FetchIndex1Name   := '~REGRESS::' + prefix + '::DG_FetchIndex1';
DG_FetchIndex2Name   := '~REGRESS::' + prefix + '::DG_FetchIndex2';
DG_FetchIndexDiffName:= '~REGRESS::' + prefix + '::DG_FetchIndexDiff';

//record structures
DG_FetchRecord := RECORD
  INTEGER8 sequence;
  STRING2  State;
  STRING20 City;
  STRING25 Lname;
  STRING15 Fname;
END;

DG_FetchFile   := DATASET(DG_FetchFileName,{DG_FetchRecord,UNSIGNED8 __filepos {virtual(fileposition)}},FLAT);
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

DG_OutRec := RECORD
    unsigned4  DG_ParentID;
    #IF(DG_MaxField>=1) string10  DG_firstname; #end
    #IF(DG_MaxField>=2) string10  DG_lastname;  #end
    #IF(DG_MaxField>=3) unsigned1 DG_Prange;    #end
    #IF(DG_MaxField>=4) string10  DG_Street;    #end
    #IF(DG_MaxField>=5) unsigned1 DG_zip;       #end
    #IF(DG_MaxField>=6) unsigned1 DG_age;       #end
    #IF(DG_MaxField>=7) string2   DG_state;     #end
    #IF(DG_MaxField>=8) string3   DG_month;     #end
END;
DG_OutRecChild := RECORD
    unsigned4  DG_ParentID;
    unsigned4  DG_ChildID;
    #IF(DG_MaxField>=1) string10  DG_firstname; #end
    #IF(DG_MaxField>=2) string10  DG_lastname;  #end
    #IF(DG_MaxField>=3) unsigned1 DG_Prange;    #end
    #IF(DG_MaxField>=4) string10  DG_Street;    #end
    #IF(DG_MaxField>=5) unsigned1 DG_zip;       #end
    #IF(DG_MaxField>=6) unsigned1 DG_age;       #end
    #IF(DG_MaxField>=7) string2   DG_state;     #end
    #IF(DG_MaxField>=8) string3   DG_month;     #end
END;

#if(DG_MaxField >= 3 AND DG_GenVar = TRUE)
DG_VarOutRec := RECORD
  DG_OutRec;
  IFBLOCK(self.DG_Prange%2=0)
    string20 ExtraField;
  END;
END;
#end

//DATASET declarations
#IF(DG_MaxField=1)
DG_BlankSet := dataset([{0,''}],DG_OutRec);
#end
#IF(DG_MaxField=2)
DG_BlankSet := dataset([{0,'',''}],DG_OutRec);
#end
#IF(DG_MaxField=3)
DG_BlankSet := dataset([{0,'','',0}],DG_OutRec);
#end
#IF(DG_MaxField=4)
DG_BlankSet := dataset([{0,'','',0,''}],DG_OutRec);
#end
#IF(DG_MaxField=5)
DG_BlankSet := dataset([{0,'','',0,'',0}],DG_OutRec);
#end
#IF(DG_MaxField=6)
DG_BlankSet := dataset([{0,'','',0,'',0,0}],DG_OutRec);
#end
#IF(DG_MaxField=7)
DG_BlankSet := dataset([{0,'','',0,'',0,0,''}],DG_OutRec);
#end
#IF(DG_MaxField=8)
DG_BlankSet := dataset([{0,'','',0,'',0,0,'',''}],DG_OutRec);
#end

#if(DG_GenFlat = TRUE OR DG_GenIndex = TRUE)
DG_FlatFile      := DATASET(DG_FileOut+'FLAT',{DG_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
DG_FlatFileEvens := DATASET(DG_FileOut+'FLAT_EVENS',{DG_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
#end
#if(DG_GenIndex = TRUE)
DG_indexFile      := INDEX(DG_FlatFile,
    RECORD
    #IF(DG_MaxField>=1) DG_firstname; #end
    #IF(DG_MaxField>=2) DG_lastname;  #end
#if(usePayload = TRUE)
    END , RECORD
#end
    #IF(DG_MaxField>=3) DG_Prange;    #end
    #IF(DG_MaxField>=4) DG_Street;    #end
    #IF(DG_MaxField>=5) DG_zip;       #end
    #IF(DG_MaxField>=6) DG_age;       #end
    #IF(DG_MaxField>=7) DG_state;     #end
    #IF(DG_MaxField>=8) DG_month;     #end
        filepos
    END,DG_FileOut+'INDEX');
DG_indexFileEvens := INDEX(DG_FlatFileEvens,
    RECORD
    #IF(DG_MaxField>=1) DG_firstname; #end
    #IF(DG_MaxField>=2) DG_lastname;  #end
#if(usePayload = TRUE)
    END , RECORD
#end
    #IF(DG_MaxField>=3) DG_Prange;    #end
    #IF(DG_MaxField>=4) DG_Street;    #end
    #IF(DG_MaxField>=5) DG_zip;       #end
    #IF(DG_MaxField>=6) DG_age;       #end
    #IF(DG_MaxField>=7) DG_state;     #end
    #IF(DG_MaxField>=8) DG_month;     #end
        filepos
    END,DG_FileOut+'INDEX_EVENS');
#end
#if(DG_GenCSV = TRUE)
DG_CSVFile   := DATASET(DG_FileOut+'CSV',DG_OutRec,CSV);
#end
#if(DG_GenXML = TRUE)
DG_XMLFile   := DATASET(DG_FileOut+'XML',DG_OutRec,XML);
#end
#if(DG_MaxField >= 3 AND DG_GenVar = TRUE)
DG_VarOutRecPlus := RECORD
  DG_VarOutRec,
  unsigned8 __filepos { virtual(fileposition)};
END;

DG_VarFile   := DATASET(DG_FileOut+'VAR',DG_VarOutRecPlus,FLAT);
DG_VarIndex  := INDEX(DG_VarFile,{DG_firstname,DG_lastname,__filepos},DG_FileOut+'VARINDEX');
DG_VarVarIndex  := INDEX(DG_VarFile,{DG_firstname,DG_lastname,__filepos},{ string temp_blob1 := TRIM(ExtraField); string10000 temp_blob2 {blob} := ExtraField },DG_FileOut+'VARVARINDEX');
#end
#if(DG_GenChild = TRUE)
DG_ParentFile  := DATASET(DG_ParentFileOut,{DG_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
DG_ChildFile   := DATASET(DG_ChildFileOut,{DG_OutRecChild,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
  #if(DG_GenGrandChild = TRUE)
DG_GrandChildFile := DATASET(DG_GrandChildFileOut,{DG_OutRecChild,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
  #end
#end

//define data atoms - each set has 16 elements
SET OF STRING10 DG_Fnames := ['DAVID','CLAIRE','KELLY','KIMBERLY','PAMELA','JEFFREY','MATTHEW','LUKE',
                              'JOHN' ,'EDWARD','CHAD' ,'KEVIN'   ,'KOBE'  ,'RICHARD','GEORGE' ,'DIRK'];
SET OF STRING10 DG_Lnames := ['BAYLISS','DOLSON','BILLINGTON','SMITH'   ,'JONES'   ,'ARMSTRONG','LINDHORFF','SIMMONS',
                              'WYMAN'  ,'MORTON','MIDDLETON' ,'NOWITZKI','WILLIAMS','TAYLOR'   ,'DRIMBAD'  ,'BRYANT'];
SET OF UNSIGNED1 DG_PrangeS := [1, 2, 3, 4, 5, 6, 7, 8,
                                9,10,11,12,13,14,15,16];
SET OF STRING10 DG_Streets := ['HIGH'  ,'CITATION'  ,'MILL','25TH' ,'ELGIN'    ,'VICARAGE','YAMATO' ,'HILLSBORO',
                               'SILVER','KENSINGTON','MAIN','EATON','PARK LANE','HIGH'    ,'POTOMAC','GLADES'];
SET OF UNSIGNED1 DG_ZIPS := [101,102,103,104,105,106,107,108,
                             109,110,111,112,113,114,115,116];
SET OF UNSIGNED1 DG_AGES := [31,32,33,34,35,36,37,38,
                             39,40,41,42,43,44,45,56];
SET OF STRING2 DG_STATES := ['FL','GA','SC','NC','TX','AL','MS','TN',
                             'KY','CA','MI','OH','IN','IL','WI','MN'];
SET OF STRING3 DG_MONTHS := ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG',
                             'SEP','OCT','NOV','DEC','ABC','DEF','GHI','JKL'];


//----------------------------- Child query related definitions ----------------------------------

// Raw record definitions:

sqHouseRec :=
            record
string          addr;
string10        postcode;
unsigned2       yearBuilt := 0;
            end;


sqPersonRec :=
            record
string          forename;
string          surname;
udecimal8       dob;
udecimal8       booklimit := 0;
unsigned2       aage := 0;
            end;

sqBookRec :=
            record
string          name;
string          author;
unsigned1       rating100;
udecimal8_2     price := 0;
            end;


// Nested record definitions
sqPersonBookRec :=
            record
sqPersonRec;
dataset(sqBookRec)      books;
            end;

sqHousePersonBookRec :=
            record
sqHouseRec;
dataset(sqPersonBookRec) persons;
            end;


// Record definitions with additional ids

sqHouseIdRec :=
            record
unsigned4       id;
sqHouseRec;
            end;


sqPersonIdRec :=
            record
unsigned4       id;
sqPersonRec;
            end;


sqBookIdRec :=
            record
unsigned4       id;
sqBookRec;
            end;


// Same with parent linking field.

sqPersonRelatedIdRec :=
            record
sqPersonIdRec;
unsigned4       houseid;
            end;


sqBookRelatedIdRec :=
            record
sqBookIdRec;
unsigned4       personid;
            end;


// Nested definitions with additional ids...

sqPersonBookIdRec :=
            record
sqPersonIdRec;
dataset(sqBookIdRec)        books;
            end;

sqHousePersonBookIdRec :=
            record
sqHouseIdRec;
dataset(sqPersonBookIdRec) persons;
            end;


sqPersonBookRelatedIdRec :=
            RECORD
                sqPersonBookIdRec;
unsigned4       houseid;
            END;

sqNestedBlob :=
            RECORD
udecimal8       booklimit := 0;
            END;

sqSimplePersonBookRec :=
            RECORD
string20        surname;
string10        forename;
udecimal8       dob;
//udecimal8     booklimit := 0;
sqNestedBlob    limit{blob};
unsigned1       aage := 0;
dataset(sqBookIdRec)        books{blob};
            END;
sqNamePrefix := '~REGRESS::' + prefix + '::';
sqHousePersonBookName := sqNamePrefix + 'HousePersonBook';
sqPersonBookName := sqNamePrefix + 'PersonBook';
sqHouseName := sqNamePrefix + 'House';
sqPersonName := sqNamePrefix + 'Person';
sqBookName := sqNamePrefix + 'Book';
sqSimplePersonBookName := sqNamePrefix + 'SimplePersonBook';

sqHousePersonBookIndexName := sqNamePrefix + 'HousePersonBookIndex';
sqPersonBookIndexName := sqNamePrefix + 'PersonBookIndex';
sqHouseIndexName := sqNamePrefix + 'HouseIndex';
sqPersonIndexName := sqNamePrefix + 'PersonIndex';
sqBookIndexName := sqNamePrefix + 'BookIndex';
sqSimplePersonBookIndexName := sqNamePrefix + 'SimplePersonBookIndex';
sqHousePersonBookIdExRec := record
sqHousePersonBookIdRec;
unsigned8           filepos{virtual(fileposition)};
                end;

sqPersonBookRelatedIdExRec := record
sqPersonBookRelatedIdRec;
unsigned8           filepos{virtual(fileposition)};
                end;

sqHouseIdExRec := record
sqHouseIdRec;
unsigned8           filepos{virtual(fileposition)};
                end;

sqPersonRelatedIdExRec := record
sqPersonRelatedIdRec;
unsigned8           filepos{virtual(fileposition)};
                end;

sqBookRelatedIdExRec := record
sqBookRelatedIdRec;
unsigned8           filepos{virtual(fileposition)};
                end;

sqSimplePersonBookExRec := record
sqSimplePersonBookRec;
unsigned8           filepos{virtual(fileposition)};
                end;

// Dataset definitions:


sqHousePersonBookDs := dataset(sqHousePersonBookName, sqHousePersonBookIdExRec, thor);
sqPersonBookDs := dataset(sqPersonBookName, sqPersonBookRelatedIdRec, thor);
sqHouseDs := dataset(sqHouseName, sqHouseIdExRec, thor);
sqPersonDs := dataset(sqPersonName, sqPersonRelatedIdRec, thor);
sqBookDs := dataset(sqBookName, sqBookRelatedIdRec, thor);

sqHousePersonBookExDs := dataset(sqHousePersonBookName, sqHousePersonBookIdExRec, thor);
sqPersonBookExDs := dataset(sqPersonBookName, sqPersonBookRelatedIdExRec, thor);
sqHouseExDs := dataset(sqHouseName, sqHouseIdExRec, thor);
sqPersonExDs := dataset(sqPersonName, sqPersonRelatedIdExRec, thor);
sqBookExDs := dataset(sqBookName, sqBookRelatedIdExRec, thor);

sqSimplePersonBookDs := dataset(sqSimplePersonBookName, sqSimplePersonBookExRec, thor);
sqSimplePersonBookIndex := index(sqSimplePersonBookDs, { surname, forename, aage  }, { sqSimplePersonBookDs }, sqSimplePersonBookIndexName);

//related datasets:
//Don't really work because inheritance structure isn't preserved.

relatedBooks(sqPersonIdRec parentPerson) := sqBookDs(personid = parentPerson.id);
relatedPersons(sqHouseIdRec parentHouse) := sqPersonDs(houseid = parentHouse.id);

sqNamesTable1 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);
sqNamesTable2 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);
sqNamesTable3 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);
sqNamesTable4 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);
sqNamesTable5 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);
sqNamesTable6 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);
sqNamesTable7 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);
sqNamesTable8 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);
sqNamesTable9 := dataset(sqSimplePersonBookDs, sqSimplePersonBookName, FLAT);

sqNamesIndex1 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
sqNamesIndex2 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
sqNamesIndex3 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
sqNamesIndex4 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
sqNamesIndex5 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
sqNamesIndex6 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
sqNamesIndex7 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
sqNamesIndex8 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);
sqNamesIndex9 := index(sqSimplePersonBookIndex,sqSimplePersonBookIndexName);

#line(0)
//UseStandardFiles
//UseIndexes

// try it with just one limit


childResultRecord := record
  STRING25 Lname;
  STRING15 Fname;
  unsigned cnt;
end;

resultRecord := record
  STRING25 Lname;
  STRING15 Fname;
  dataset(childResultRecord) children;
end;

resultRecord t1(DG_FetchIndex1 l) := transform
    deduped := table(dedup(DG_FetchIndex1(LName != l.FName), LName, ALL), { __filepos, LName, FName });
    cntDedup := count(deduped);

    childResultRecord t2(deduped l, DG_FetchIndex1 r, unsigned cnt) := transform
        SELF := l;
        SELF.cnt := cnt;
        END;

    self := l;
    self.children := JOIN(deduped, DG_FetchIndex1, left.LName = right.LName and left.FName = right.FName and right.__filepos != cntDedup, t2(left, right, 0));
    end;

p1 := projectmight (DG_FetchIndex1, t1(left));
output(p1);



