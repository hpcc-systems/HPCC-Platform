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


r := record
  string line;
  end;

ds := dataset([
{'ii iv ix  viii viiii'},
{''}],r);


PATTERN i := REPEAT('I', 1, 3);
PATTERN iv := 'IV';
PATTERN ix := 'IX';
PATTERN v := 'V';
PATTERN x := REPEAT('X', 1, 3);
PATTERN xl := 'XL';
PATTERN xc := 'XC';
PATTERN l := 'L';
PATTERN c := REPEAT('C', 1, 3);
PATTERN cd := 'CD';
PATTERN cm := 'CM';
PATTERN d := 'D';
PATTERN m := REPEAT('M', 1, 3);
PATTERN roman_digits := ['I', 'V', 'X', 'L', 'C', 'D', 'M']+;
PATTERN roman_sequence :=  m? (cm? | (d? c? | cd?)) (xc? | (l? x? | xl?)) (ix? | (v? i? | iv?));
PATTERN roman_numeral := roman_digits in roman_sequence;

pattern S := roman_numeral;


results :=
    record
        string outline := MATCHTEXT;
    end;

output(PARSE(ds,line,S,results,nocase,max,many));

