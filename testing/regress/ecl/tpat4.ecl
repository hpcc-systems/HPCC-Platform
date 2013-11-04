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
        string tree := 'Tree: '+lib_parselib.parseLib.getParseTree();
    end;

outfile1 := PARSE(d,line,S,results,first,whole,skip(' '),parse,matched(all));

output(outfile1);

