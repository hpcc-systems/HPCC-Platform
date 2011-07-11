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



set of string searchSet := [] : stored('searchSet');
string matchName := '' : stored('matchName');


pattern word := pattern('[a-z]+');
pattern ws := [' ','/t'];
pattern match := validate(word, MATCHTEXT in searchSet or MATCHTEXT = matchName);
pattern S := match ws match;


ds := dataset([{'Gavin Halliday'}],{string line});


x := parse(ds, line, S, TRANSFORM(LEFT));

output(x);
