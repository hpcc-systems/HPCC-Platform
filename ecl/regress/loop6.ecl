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

