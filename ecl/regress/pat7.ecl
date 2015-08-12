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

token patWord := PATTERN('[[:alpha:]]')+;
pattern punct := [' ',',',';','\t','.']*;

token noun := patWord;
token target1 := 'Gavin';
token target2 := 'Nigel';
token pronoun := ['i','he','you','we','they'];

token subject := noun;// in pronoun;
rule action1 := patWord patWord target1;
rule action2 := subject patWord target2;

rule phrase := action1 | action2;

infile := dataset([
        {'I hate gavin.'},
        {'he hates Nigel'}
        ], { string line });


results :=
    record
        MATCHTEXT(patWord[1]) + ',' + MATCHTEXT(patWord[2]) + ',' + MATCHTEXT(target1)+ ',' + MATCHTEXT(target2);
    end;

output(PARSE(infile,line,phrase,results,scan,nocase,skip(punct)));
