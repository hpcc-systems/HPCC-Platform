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

import lib_parselib;
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
  | use(VP) use(PP)
  ;

rule PP := p NP;
 
rule S := NP VP
  | self VP
  ;



results := 
    record
        lib_parselib.parseLib.getXmlParseTree();
    end;

outfile1 := PARSE(ds,line,S,results,skip(ws),parse,whole,use(pp),matched(ALL));
//output(outfile1,,'nlp.xml',csv,overwrite);
output(outfile1);
