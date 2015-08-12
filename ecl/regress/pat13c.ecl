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

pattern ws := pattern('[[:space:]]');
token d := ['the', 'a'];
token p := ['in', 'with'];
token n := ['girl', 'boy', 'park', 'telescope', 'saw'];
token v := ['saw'];

//Requirements for use is that parameter is used further up the tree, or is specified as a use(x) attribute on the parse.
//convert : define('xx') to no_pat_define(delta, 'xx');
//rule NP := d n | NP PP;
rule NP2 := | use('PP') SELF;               // both forms of use()
rule NP := (d n) NP2;
//rule VP := v NP | VP PP;
rule VP2 := use(PP) SELF | ;                // use SELF and one form of use.
rule VP := (v NP) VP2;
rule PP := define(p NP, 'PP');
rule S  := NP VP;


infile := dataset([
        {'the boy saw the girl'},
        {'the boy saw the girl in the park'},                       // ambiguous...
        {'the boy saw the girl in the park with a telescope'},      // ambiguous
        {''}
        ], { string line });


results :=
    record
        infile.line+': ';
        parseLib.getXmlParseTree();
    end;


//Return first matching sentance that we can find
outfile1 := PARSE(infile,line,S,results,all,skip(ws*),matched(all),use(PP),whole);
output(outfile1);
