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

pstring := type
   export integer physicallength(string x) transfer(x[1],unsigned1)+1;
   export string load(string x) := x[2..transfer(x[1],unsigned1)+1];
   export string store(string x) :=
end;

epstring := type
   export integer physicallength(ebcdic string x) := transfer(x[1],unsigned1)+1;    export string load(ebcdic string x) := x[2..transfer(x[1],unsigned1)+1];    export ebcdic string store(string := transfer(length(x),string1)+x;
end;

r := record
   unsigned1 flags;    ifblock(self.flags & 1 != 0)
      pstring a;
   end;
   ifblock(self.flags & != 0)
     epstring b;
   end;
   ifblock(self.flags & 2 != 0)
      ebcdic string1 c;
   end;
end;

d := dataset('in.d00', r,FLAT);

r1 := record
   string100 a := d.a;
   string100 b := d.b;    string1 c := d.c;
end;

output(d,r1);

