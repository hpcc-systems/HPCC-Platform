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

token patWord(pattern quoteText) := quoteText PATTERN('[a-z]+') quoteText;
token p1 := patWord('$');
token p2 := patWord('!');
rule sentance := p1 repeat(p2);

//This needs to be allowed, but requires types to be sorted out ..
rule sentance2 := patWord('$') repeat(patWord('!'));

infile := dataset([
        {'$one$ !two! three'},
        {'gavin $Hawthorn$'},
        {''}
        ], { string line });


results :=
    record
        parseLib.getParseTree();
        MATCHED(p1);                        //patWord('$')[1];
        MATCHED(p2);
        MATCHTEXT(p1);
        MATCHUNICODE(p1);
        MATCHLENGTH(p1);
        MATCHPOSITION(p1);
    end;


//Return first matching sentance that we can find
outfile1 := PARSE(infile,line,sentance,results,first,scan,skip(ws*));
output(outfile1);

results2 :=
    record
        parseLib.getParseTree();
        MATCHTEXT;
    end;


//Return first matching sentance that we can find
output(PARSE(infile,line,sentance2,results2,first,scan,skip(ws*)));
