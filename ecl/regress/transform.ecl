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


