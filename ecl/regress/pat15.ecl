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

infile := dataset([
        {'Gavin, Mia and Abigail and Nathan'}
        ], { string line });

pattern ws := ' ' | '\t';
token name := pattern('[a-z]+');
token comma := ',';
token t_and := 'and';


rule listNext
    := comma name
    |  t_and name
    ;

rule listContinue
    := listNext self
    |
    ;

rule listFull := name listContinue;

rule S := listFull;

results :=
    record
        infile.line+': ';
        parseLib.getParseTree();
    end;


//Return first matching sentance that we can find
outfile1 := PARSE(infile,line,S,results,all,skip(ws*),whole,nocase);
output(outfile1);
