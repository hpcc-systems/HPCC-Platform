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

//See pp229-> of dragon.  This generates a s/r conflict from the SLR tables.

rec := record
  string line;
  end;

d := dataset([
{'left = right'},
{'*left = right'},
{'dcccd'},
{''}],rec);


//Example comes from Dragon pp218->

rule R := use(L);

rule L := '*' R
        | pattern('[a-z]*')
        ;

rule S := L '=' R
        | R
        ;

results := 
    record
        string Le :=  '!'+MATCHTEXT(S)+'!';
        string tree := 'Tree: '+parseLib.getParseTree();
    end;

outfile1 := PARSE(d,line,S,results,first,whole,skip(' '),parse,matched(all));

output(outfile1);

