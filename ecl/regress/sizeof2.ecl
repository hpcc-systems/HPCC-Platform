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

// test compiling time error of sizeof

//==== bitfield ===
n1 := sizeof(bitfield1);
n2 := sizeof(bitfield8);

bitfield2 bf := 1;
n3 := sizeof(bf);

s := [1,2,3];
n4 := sizeof(s);
n4x := sizeof(all);
n4y := sizeof([]);
n4z := sizeof(['a','b']);
n5 := sizeof(set of integer1);

//== varstring ====
string s1 := 'abcd';
n6 := sizeof(s1);
n7 := sizeof(string);

varstring vs := 'abcd';
n8 := sizeof(vs);
n9 := sizeof(varstring);

//== alientype ===
VariableString(integer len) := TYPE
  export integer   physicalLength(string physical) := (integer)len;
  export string    load(string physical) := physical;
  export string    store(string logical) := logical;
END;

// more:
// VariableString(10) vvss;
// n10 := sizeof(vvss);
n10x := sizeof(VariableString(10));

//=== record === 

r1 := record  
   string2 sx;
   IFBLOCK(true)
      boolean bx;
   END;
end;

n11 := sizeof(r1);

r2 := record
   string2 sx;
   bitfield3 bfx;
END;

n12 := sizeof(r2);

r3 := record
   VariableString(10) vsx;
end;
 
n13 := sizeof(r3);
