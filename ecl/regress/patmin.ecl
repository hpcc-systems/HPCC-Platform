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

layout := {
   string s {maxlength(100)},
};

ds := dataset([{'<abcd>'},{'<abcdefghijklmn>'}], layout);

pattern letter := pattern('[a-z]');
pattern letterpair := pattern('[a-z][a-z]');

pattern prelettersmin := repeat(letter, 0, 5, MIN);
pattern prelettersmax := repeat(letter, 0, 5);
pattern letters := repeat(letter, 1, any);

pattern elementmin := '<' preLettersmin letters'>';
pattern elementmax := '<' preLettersmax letters'>';

pattern prepairsmin := repeat(letterpair, 0, 5, MIN);
pattern prepairsmax := repeat(letterpair, 0, 5);
pattern pairs := repeat(letterpair, 1, any);

pattern elementpairmin := '<' prePairsmin pairs '>';
pattern elementpairmax := '<' prePairsmax pairs'>';


layout parseLetters(layout rec) := transform
   self.s := matchtext(letters);
end;

layout parsePairs(layout rec) := transform
   self.s := matchtext(pairs);
end;


output(parse(ds, s, elementmin, parseLetters(left)), named('Letter_min'));
output(parse(ds, s, elementmax, parseLetters(left)), named('Letter_max'));

output(parse(ds, s, elementpairmin, parsePairs(left)), named('pair_min'));
output(parse(ds, s, elementpairmax, parsePairs(left)), named('pair_max'));
