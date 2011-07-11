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
