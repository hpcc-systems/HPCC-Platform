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

//See pp231-> of dragon.

r := record
  string line;
  end;

d := dataset([
{'ccccdcccd'},
{'cccd'},
{'dcccd'},
{''}],r);


//Example comes from Dragon pp218->

rule C := 'c' self
        | 'd'
        ;

rule S := C C
        ;

results :=
    record
        string Le :=  '!'+MATCHTEXT(S)+'!';
        string tree := 'Tree: '+parseLib.getParseTree();
    end;

outfile1 := PARSE(d,line,S,results,first,whole,parse);

output(outfile1);

