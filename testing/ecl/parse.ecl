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

import lib_parselib;
//See pp219-> of dragon.

r := record
  string line;
  end;

d := dataset([
{'abc * def + ghi  '},
{'(a + b) * (c + d)'},
{''}],r);


//Example comes from Dragon pp218->

pattern ws := [' ','\t',',']*;
pattern id := PATTERN('[a-zA-Z]+');

rule F := '(' use(E) ')'
        | PATTERN('[a-zA-Z]+')
        ;

rule T := self '*' F
        | F
        ;

rule E := use(E) '+' T
        | T
        ;

results := 
    record
        string Le :=  '!'+MATCHTEXT(E)+'!';
//      string tree := 'Tree: '+parseLib.getParseTree();
//      string tree2 := 'Xml: '+parseLib.getXmlParseTree();
    end;

outfile1 := PARSE(d,line,E,results,first,whole,skip(ws),parse);
output(outfile1);

outfile2 := PARSE(d,line,E,results,all,scan all,skip(ws),parse);    // should display all partial values.
output(outfile2);
