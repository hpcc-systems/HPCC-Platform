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
