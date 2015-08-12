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



d := dataset([
    { 'Gavin begat Nathan' },
    { 'The son of God'}
    ], { string line });

pattern ws := [' ','\t'];
pattern patStart := FIRST | ws;
pattern patEnd := LAST | ws;
article := ['A','The','Thou'];
token patWord := PATTERN('[a-zA-Z]+');
// Line below
token Namet := PATTERN('[A-Z][a-zA-Z]+') NOT IN article;
pattern produced := ['begat','son of','father of'];
rule progeny := Namet ws produced ws Namet;
results :=
    record
        Le :=  MATCHTEXT(Namet[1]);
        Ri :=  MATCHTEXT(Namet[2]);
        RelationType := MatchText(produced[1]);
        string8 head := d.line[1..8];
    end;

outfile1 := PARSE(d,line,progeny,results);
output(choosen(outfile1,1000))
