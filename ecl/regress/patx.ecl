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

token patWord := PATTERN('[a-z]+'); // deliberately lower case only
rule sentance := patStart patWord repeat(ws patWord) patEnd;
rule sentance2 := repeat(patWord);

infile := dataset([
{'one two three'},
{'Gavin Hawthorn'},
{'I went out I went in'},
{''}
], { string line });


results :=
record
MATCHED(patWord[1]);
MATCHTEXT(patWord[1]);
MATCHLENGTH(patWord[1]);
MATCHPOSITION(patWord[1]);
MATCHED(patWord[8]);
MATCHTEXT(patWord[8]);
MATCHLENGTH(patWord[8]);
MATCHPOSITION(patWord[8]);
end;


//Return first matching sentance that we can find
outfile1 := PARSE(infile,line,sentance,results);
output(outfile1);

//Return all matching sentances, case insignficant.
outfile2 := PARSE(infile,line,sentance,results,nocase,scan);
output(outfile2);

//Return first matching sentance that we can find
outfile3 := PARSE(infile,line,sentance2,results,skip(ws*));
output(outfile3);

//Return all matching sentances, case insignficant.
outfile4 := PARSE(infile,line,sentance2,results,nocase,scan,skip(ws*));
output(outfile4);
