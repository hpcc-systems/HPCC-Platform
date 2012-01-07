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

token word := PATTERN('[a-z0-9]')+;
token a := word;
token b := word;
rule c := a b;
rule d := a b;
rule e := c;
rule f := e d;
rule g := f f;

//i.e.
//g[f[e[c[a b]] d[a b]] f[e[c[a b]] d[a b]]]

infile := dataset([
        {'w1 w2 w3 w4 w5 w6 w7 w8'}
        ], { string line });


results :=
    record
        '\'' + MATCHTEXT(word) + ':w1\'';
        '\'' + MATCHTEXT(word[2]) + ':w2\'';
        '\'' + MATCHTEXT(word[8]) + ':w8\'';
        '\'' + MATCHTEXT(word[9]) + ':\'';
        '\'' + MATCHTEXT(a/word[1]) + ':w1\'';
        '\'' + MATCHTEXT(a[3]/word[1]) + ':w5\'';
        '\'' + MATCHTEXT(a[3]/word[2]) + ':\'';
        '\'' + MATCHTEXT(a[2]/word) + ':w3\'';
        '\'' + MATCHTEXT(e/a[2]/word) + ':w5\'';
        '\'' + MATCHTEXT(g/f/e/c/a/word) + ':w1\'';
        '\'' + MATCHTEXT(g/f[2]/e/c/a/word) + ':w5\'';
        '\'' + MATCHTEXT(f[1]/e/c/b/word) + ':w2\'';
        '\'' + MATCHTEXT(f[1]/b[2]/word) + ':w4\'';
        '\'' + MATCHTEXT(f[1]/c/b[2]/word) + ':\'';
        '\'' + MATCHTEXT(f[1]/c/b[3]/word) + ':\'';
        '\'' + MATCHTEXT(g/f[1]/b[2]/word) + ':w4\'';
        '\'' + MATCHTEXT(g/f[1]/c/b[3]/word) + ':\'';
        '\'' + MATCHTEXT(a/word[3]) + ':w5\'';
    end;

output(PARSE(infile,line,g,results,whole,nocase,skip([' ',',',';','\t','.']*)));
