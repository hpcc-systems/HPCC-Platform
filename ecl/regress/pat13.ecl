/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
rule PP := p NP                             : define('PP');
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
