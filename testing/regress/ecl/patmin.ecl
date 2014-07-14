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
