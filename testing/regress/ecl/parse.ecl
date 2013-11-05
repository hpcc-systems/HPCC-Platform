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
