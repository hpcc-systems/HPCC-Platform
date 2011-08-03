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

//See pp219-> of dragon.

r := record
  string line;
  end;

d := dataset([
{'abc * def + ghi'},
{'(a + b) * (c + d)'},
{''}],r);


//Example comes from Dragon pp218->

pattern ws := [' ','\t',',']*;
token id := PATTERN('[a-zA-Z]+');

rule beforeId := pattern('');
rule afterId := pattern('');
rule E := beforeId id afterId;


results :=
    record
        beforePos := matchposition(beforeId);
        afterPos := matchposition(afterId);
        x := MATCHTEXT(id)+'!';
    end;

outfile1 := PARSE(d,line,E,results,skip(ws),scan,parse);
output(outfile1);
