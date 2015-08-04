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
