/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

IMPORT * FROM lib_parselib;

r := record
  string10000 line;
//  unsigned integer8 __filepos { virtual(fileposition)};
  end;
dp := dataset('~thor_data50::in::ktext',r,csv(separator('')));

r roll(r le, r ri) := transform
  self.line := trim(le.line)+' '+trim(ri.line);
  end;

//d := choosen(rollup(dp,right.line[4]<>':',roll(left,right)), 200);

//d := dataset([{'Ge 34:2 And when Shechem the son of Hamor the Hivite, prince of the country, saw her, he took her, and lay with her, and defiled her.'}],r);

 d := dataset([
{'Ge 34:2 And when Shechem the son of Hamor the Hivite, prince of the country, saw her, he took her, and lay with her, and defiled her.'},
{'Ge 36:10 These are the names of Esaus sons; Eliphaz the son of Adah the wife of Esau, Reuel the son of Bashemath the wife of Esau.'},
{'Ge 34:2 And when Shechem the son of Hamor the Hivite, prince of the country, saw her, he took her, and lay with her, and defiled her.'}],r);

pattern ws := [' ','\t',',']*;
pattern patStart := FIRST | ws;
pattern patEnd := LAST | ws;
pattern article := ['A','The','Thou','a','the','thou'];
pattern patWord := PATTERN('[a-zA-Z]+');        
pattern Name := PATTERN('[A-Z][a-zA-Z]+');  
pattern Namet := name opt(ws 'the' ws name);
pattern produced := opt(article ws) ['begat','father of','mother of'];
pattern produced_by := opt(article ws) ['son of','daughter of'];
pattern produces_with := opt(article ws) ['wife of'];
rule progeny := namet ws ( produced | produced_by | produces_with ) ws namet;
results := 
    record
        string Le :=  '!'+MATCHTEXT(Namet[1])+'!';
        string Ri :=  '!'+MATCHTEXT(Namet[2])+'!';
        string ParentPhrase := '!'+MatchText(produced[1])+'!';
        string ChildPhrase := '!'+MatchText(produced_by[1])+'!';
        string SpousePhrase := '!'+MatchText(produces_with[1])+'!';
        string tree := 'Tree: '+parseLib.getParseTree();
    end;

outfile1 := PARSE(d,line,progeny,results,scan all,matched(Name));// : persist('kjv::relationships');

//output(d);
/*results switch(results le) := transform
  self.le := le.ri;
  self.ri := le.le;
  self := le;
  end;

kids := project(outfile1(childphrase<>''),switch(left));

p_claims := outfile1(parentphrase<>'')+kids ;

results2 := record
  string parent := p_claims.le;
  string child := p_claims.ri;
  unsigned4 cnt := count(group);
  end;

t := table(p_claims,results2,le,ri);*/

//output(t); 
count(outfile1);
output(choosen(outfile1,1000));

