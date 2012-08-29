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

pattern patTerminator := ['.','?',LAST];

pattern sentance := repeat(any,min) patTerminator;

infile := dataset([
        {'One.Two?Three.'},
        {'Not finished'}
        ], { string text });


results := 
    record
        MATCHTEXT;
    end;

output(PARSE(infile,text,sentance,results,first,scan));     // One. Two? Three.
output(PARSE(infile,text,sentance,results,first,scan all)); // One. ne. e. . Two? wo? o? ? Three. hree. ree. ee. e. .
output(PARSE(infile,text,sentance,results,noscan,all));     // One. One.Two? One.Two?Three
output(PARSE(infile,text,sentance,results,scan,all));   // One. One.Two? One.Two?Three Two? Two?Three Three

