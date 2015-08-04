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

pattern patWord := PATTERN('[a-z]+');       // deliberately lower case only
pattern patInvalid := PATTERN(U'[\u01ff-\ufffe]+');

pattern next := ws | patWord | patInvalid;

infile := dataset([
        {U'one two three'},
        {U'Gavin Hawthorn'},
        {U'I \u01ff\u31ffout I went in'},
        {U''}
        ], { unicode line });


results :=
    record
        MATCHED(patWord[1]);
        MATCHED(patInvalid);
        MATCHPOSITION(next);
        MATCHUNICODE(patWord);
    end;


//Return first matching sentance that we can find
outfile1 := PARSE(infile,line,next,results,FIRST);
output(outfile1);

