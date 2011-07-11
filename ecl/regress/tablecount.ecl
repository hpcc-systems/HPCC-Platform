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
