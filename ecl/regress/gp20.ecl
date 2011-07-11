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

#option ('optimizeGraph', false);
#option ('foldAssign', false);
#option ('globalFold', false);

stringextract(const string src, integer x) := src[x..x+0];

namesRecord := 
            RECORD
string20        per_street;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset([
        {'Halliday','Gavin',31},
        {'Halliday','Liz',30},
        {'Salter','Abi',10},
        {'X','Z'}], namesRecord);


st :=
  RECORD
  namesTable.per_street;
  END;

ps := table(choosen(namesTable,100),st);

st remcom(st l) := transform
self.per_street :=(string45)stringextract(l.per_street,1)+' '+
(string45)stringextract(l.per_street,2)+' '+
(string45)stringextract(l.per_street,3);
self := l;
end; 

p := project(ps,remcom(left));

output(p,,'out.d00')

/*
If you take the cast to string45 outside of the concats it works fine
....

st :=
  RECORD
  person.per_street;
  END;

ps := table(choosen(person,100),st);

st remcom(st l) := transform
self.per_street :=(string45)(stringlib.stringextract(l.per_street,1)+'
'+
stringlib.stringextract(l.per_street,2)+' '+
stringlib.stringextract(l.per_street,3));
self := l;
end; 

p := project(ps,remcom(left));

output(p)
*/
