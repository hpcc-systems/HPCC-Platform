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
//  unsigned integer8 __filepos { virtual(fileposition)};
  end;
dp := dataset('~thor::in::ktext',r,csv(separator('')));

r roll(r le, r ri) := transform
  self.line := trim(le.line)+' '+trim(ri.line);
  end;

d := choosen(rollup(dp,right.line[4]<>':',roll(left,right)), 200);

//d := dataset([{'Ge 34:2 And when Shechem the son of Hamor the Hivite, prince of the country, saw her, he took her, and lay with her, and defiled her.'}],r);

/* d := dataset([
{'Ge 34:2 And when Shechem the son of Hamor the Hivite, prince of the country, saw her, he took her, and lay with her, and defiled her.'},
{'Ge 36:10 These are the names of Esaus sons; Eliphaz the son of Adah the wife of Esau, Reuel the son of Bashemath the wife of Esau.'},
{'Ge 34:2 And when Shechem the son of Hamor the Hivite, prince of the country, saw her, he took her, and lay with her, and defiled her.'}],r);
*/
pattern ws := [' ','\t',',']*;
token article := ['A','The','Thou','a','the','thou'];
token patWord := PATTERN('[a-zA-Z]+');
token Name := PATTERN('[A-Z][a-zA-Z]+');
rule Namet := name opt('the' name);
rule produced := opt(article) ['begat','father of','mother of'];
rule produced_by := opt(article) ['son of','daughter of'];
rule produces_with := opt(article) ['wife of'];
rule progeny := namet ( produced | produced_by | produces_with ) namet;

results :=
    record
        string Le :=  '!'+MATCHTEXT(Namet[1])+'!';
        string Ri :=  '!'+MATCHTEXT(Namet[2])+'!';
        string ParentPhrase := '!'+MatchText(produced[1])+'!';
        string ChildPhrase := '!'+MatchText(produced_by[1])+'!';
        string SpousePhrase := '!'+MatchText(produces_with[1])+'!';
        string tree := 'Tree: '+parseLib.getParseTree();
    end;

outfile1 := PARSE(d,line,progeny,results,scan all,skip(ws),terminator([' ','\t',',']),parse);// : persist('kjv::relationships');

count(outfile1);
output(choosen(outfile1,1000));

