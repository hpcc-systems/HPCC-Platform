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

r := record
  string line;
  end;

ds := dataset([
{'I saw the girl in the park with the saw'},
{'I saw a man on the bed in the apartment with the telescope'},
{''}],r);


pattern ws := [' ','\t'];
token d := ['the','a','an'];
token p := ['in','with','on'];
token n := ['girl','boy','park','telescope','saw','I','man','bed','apartment'];
token v := ['saw'];

rule NP := n
    | d n
    | use(NP) use(PP)
    ;

rule VP := v NP
  | use(VP) penalty(1) use(PP)      // make pp bind to noun if possible.
  ;

rule PP := p NP;

rule S := NP VP
  | self VP
  ;



results :=
    record
        parseLib.getXmlParseTree();
    end;

outfile1 := PARSE(ds,line,S,results,skip(ws),parse,whole,best,use(pp),matched(all));
//output(outfile1,,'nlp.xml',csv,overwrite);
output(outfile1);//,,'nlp.xml',csv,overwrite);
