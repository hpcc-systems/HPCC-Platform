/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#option ('globalFold', false);
#option ('targetClusterType', 'roxie');

//define constants
DG_GenFlat  := true;    //TRUE gens FlatFile
DG_GenIndex := true;    //TRUE gens FlatFile AND the index
DG_GenCSV   := true;    //TRUE gens CSVFile
DG_GenXML   := true;    //TRUE gens XMLFile
DG_GenVar   := true;    //TRUE gens VarFile only IF MaxField >= 3

//total number of data records generated = DG_MaxElement raised to the DG_MaxField power
//                                         maximum is 4,294,967,296 recs -- 16 to the 8th power
DG_MaxElement := 4; //base     - maximum (1 to 16) number of set elements to use building the data records
DG_MaxField   := 3;     //exponent - maximum (1 to 8) number of fields to use building the data records
DG_FileOut    := 'REGRESS::DG_'+DG_MaxElement+'_'+DG_MaxField+'_';

//record structures
DG_OutRec := RECORD
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
//DG_BlankSet  := DATASET([{'','',0,'',0,0,'',''}],DG_OutRec);

DG_FlatFile      := DATASET(DG_FileOut+'FLAT',{DG_OutRec,UNSIGNED8 filepos{virtual(fileposition)}},FLAT);
DG_indexFile      := INDEX(DG_FlatFile,{DG_FlatFile},DG_FileOut+'INDEX');

//UseStandardFiles
//UseIndexes

Layout_DG_Totals := RECORD
  DG_IndexFile.DG_FirstName;
  DG_IndexFile.DG_lastName;
                UNSIGNED Counts := COUNT(GROUP);
                END;

DG_Totals := table(DG_IndexFile,Layout_DG_Totals,DG_FirstName,DG_LastName,FEW);

output(DG_Totals)
