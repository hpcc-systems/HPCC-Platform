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


R1 := RECORD
INTEGER4 unique_id;
STRING15 field1;
STRING20 field2;
END;

RS1 := DATASET('x',R1,FLAT);

// This now defines the child record
R2 := RECORD
STRING15 field1;
STRING20 field2;
END;

// Looks just like R1 with unique_id separated and a child record field
R3 := RECORD
INTEGER4 unique_id;
R2 childrec;
END;

// RS3 then becomes the child recordset to be joined eventually to a
//parent defined with a child DATASET definition in its combined record
//layout, using the DENORMALIZE function

R3 MyTrans(R1 L) := transform
  SELF.unique_id := 0;
  SELF.childrec := L; //????
  END;


RS3 := PROJECT(RS1,MyTrans(Left));
