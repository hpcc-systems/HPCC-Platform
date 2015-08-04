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
  export integer physicallength(string x) := transfer(x[1],unsigned1)+1;
  export string load(string x) := x[2..transfer(x[1],unsigned1)+1];
  export string store(string x) := transfer(length(x),string1)+x;
  end;

in1 := record
     string10 a;
     end;


in2 := record
     string10 a;
     unsigned4 n;
     string10 o;
     pstring p;
     end;


outr := record
string10    a;
            ifblock(self.a = 'Gavin')
unsigned4       n;
string10        o;
pstring         p
            end;
        end;

in1Table := dataset('in1',in1,FLAT);
in2Table := dataset('in2',in2,FLAT);


outr JoinTransform (in1 l, in2 r) :=
                TRANSFORM
                    SELF.a := l.a;
                    SELF.n := r.n;
                    SELF := r;
                END;

outTable := join (in1Table, in2Table, LEFT.a = RIGHT.a, JoinTransform (LEFT, RIGHT));


projectrec :=   record
pstring             p := outTable.p;
unsigned1           f := 1;
                    ifblock((self.f & 1)<> 0)
string10                o := outTable.o;
                    end;
                end;

//output(outTable,,'out.d00');
output(outTable,projectrec,'out.d00');


