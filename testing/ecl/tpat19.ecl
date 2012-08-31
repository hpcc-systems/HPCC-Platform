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

output(PARSE(infile,line,g,results,whole,nocase,skip([' ',',',';','\t','.']*), parse));
