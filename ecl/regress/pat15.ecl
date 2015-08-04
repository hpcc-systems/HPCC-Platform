/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

infile := dataset([
        {'Gavin, Mia and Abigail and Nathan'}
        ], { string line });

pattern ws := ' ' | '\t';
token name := pattern('[a-z]+');
token comma := ',';
token t_and := 'and';


rule listNext
    := comma name
    |  t_and name
    ;

rule listContinue
    := listNext self
    |
    ;

rule listFull := name listContinue;

rule S := listFull;

results :=
    record
        infile.line+': ';
        parseLib.getParseTree();
    end;


//Return first matching sentance that we can find
outfile1 := PARSE(infile,line,S,results,all,skip(ws*),whole,nocase);
output(outfile1);
