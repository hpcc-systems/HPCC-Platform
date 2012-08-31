/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

ParentRec := RECORD
    INTEGER1  NameID;
    STRING20  Name;
END;
ParentTable := DATASET([ {1,'Gavin'},{2,'Mia'},{3,'Mr Nobody'},
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
