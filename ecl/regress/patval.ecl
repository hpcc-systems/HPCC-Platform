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

#option ('targetClusterType', 'roxie');

x := dataset([], { string x}) : stored('x');

//set of string searchWords := [] : stored('searchWords');
set of string searchWords := set(x, x);
set of string badWords := [] : stored('badWords');


pattern ws := [' ','\t'];
pattern patStart := FIRST | ws;
pattern patEnd := LAST | ws;

token patWord := PATTERN('[a-z]+');     // deliberately lower case only
rule sentance := patStart validate(patWord, matchtext not in searchWords) validate(patWord, matchtext not in badWords) patEnd;
rule sentance2 := patWord+;

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
        MATCHUNICODE(patWord[1]);
        MATCHLENGTH(patWord[1]);
        MATCHPOSITION(patWord[1]);
        MATCHED(patWord[8]);
        MATCHTEXT(patWord[8]);
        MATCHLENGTH(patWord[8]);
        MATCHPOSITION(patWord[8]);
    end;



//Return first matching sentance that we can find
outfile1 := PARSE(infile,line,sentance,results,first,noscan);
output(outfile1);


/*
infile2 := dataset('x', { dataset({string text}) child }, thor);

p := table(infile2, { dataset PARSE(infile2.child,text,sentance,results,first,noscan) });
output(p);
*/