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

input := dataset('ds', { string text{maxlength(1000)}; }, thor);
patterns := dataset('ds', { string phrase{maxlength(1000)}; }, thor);


projected := recordof(input) or { boolean matches; };
in2 := project(input, transform(projected, self := left; self := []));




x1 := join(input, patterns, REGEXFIND(right.phrase, LEFT.text), transform(projected, self := left; self.matches := right.phrase <>''), all);

output(x1);


checkPattern(dataset(projected) in, string search) := function

    in maybeMatch(recordof(in) l) := transform
        self.matches := REGEXFIND(search, l.text) OR l.matches;
        self := l;
    END;

    RETURN PROJECT(in, maybeMatch(LEFT));
END;

x2 := LOOP(in2, count(patterns), checkPattern(rows(left), patterns[counter].phrase));

output(x2);

