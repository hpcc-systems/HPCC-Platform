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

ParentRec := RECORD
    INTEGER1  NameID;
    STRING20  Name;
END;
ParentTable := DATASET([ {1,'Gavin'},{2,'Liz'},{3,'Mr Nobody'},
                {4,'Anywhere'}], ParentRec);    

ChildRec := RECORD
    INTEGER1  NameID;
    STRING20  Addr;
END;
ChildTable := DATASET([{1,'10 Malt Lane'},  
               {2,'10 Malt Lane'},  
               {2,'3 The cottages'},    
               {4,'Here'},  
               {4,'There'}, 
               {4,'Near'},  
               {4,'Far'}],ChildRec);    

DenormedRec := RECORD
    INTEGER1   NameID;
    STRING20   Name;
    UNSIGNED1  NumRows;
    DATASET(ChildRec, COUNT(SELF.numRows)) Children;
END;

DenormedRec ParentMove(ParentRec L) := TRANSFORM
  SELF.NumRows := 0;
  SELF.Children := [];
  SELF := L;
END;  

ParentOnly := PROJECT(ParentTable, ParentMove(LEFT));
        
DenormedRec ChildMove(DenormedRec L, ChildRec R, INTEGER C) := TRANSFORM
  SELF.NumRows := C;
  SELF.Children := L.Children + R;   
  SELF := L;
END;

DeNormedRecs := DENORMALIZE(ParentOnly, ChildTable,
                            LEFT.NameID = RIGHT.NameID,
                            ChildMove(LEFT,RIGHT,COUNTER));

OUTPUT(DeNormedRecs,,'out.d00');
