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


pattern ws := [' ','\t'];
pattern patStart := FIRST | ws;
pattern patEnd := LAST | ws;

token patWord := PATTERN('[a-z]+');     // deliberately lower case only
rule sentance := patStart patWord repeat(ws patWord) patEnd;
rule sentance2 := patWord+;

stringRec := { string value; };
inRec := { string line, dataset(stringRec) search; };

infile := dataset([
        {'one two three', [{'one'}] },
        {'Gavin Hawthorn', [{'one'}] },
        {'I went out I went in', [{'I'}] }
        ], inRec);


//test matched inside a sub query - to ensure it is evaluated in the correct place

results :=
    record
        infile.line;
        exists(dedup(infile.search, value)(value = MATCHTEXT(patWord[1])));
    end;


//Return first matching sentance that we can find
outfile1 := PARSE(infile,line,sentance,results,first,noscan);
output(outfile1);
