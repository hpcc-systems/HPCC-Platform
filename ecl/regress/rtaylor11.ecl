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

d := nofold(dataset([
    {'407WDISNEY '},
    {'800GETCOKEAH '},
    {'800GETCOKE'}
    ],{string number}));

TranslateLetter(string1 input) :=
  map(input in ['0','1','2','3','4','5','6','7','8','9'] => input,
      (input) in ['A','B','C'] => '2',
      (input) in ['D','E','F'] => '3',
      (input) in ['G','H','I'] => '4',
      (input) in ['J','K','L'] => '5',
      (input) in ['M','N','O'] => '6',
      (input) in ['P','Q','R','S'] => '7',
      (input) in ['T','U','V'] => '8',
      (input) in ['W','X','Y','Z'] => '9',
            ''); //or you could default to return the input if anything else

TranslateLetter2(string1 input) :=
  map((input) in ['0']=>'0',
      (input) in ['1']=>'1',
      (input) in ['2']=>'2',
      (input) in ['3']=>'3',
      (input) in ['4']=>'4',
      (input) in ['5']=>'5',
      (input) in ['6']=>'6',
      (input) in ['7']=>'7',
      (input) in ['8']=>'8',
      (input) in ['9']=>'9',
      (input) in ['A','B','C'] => '2',
      (input) in ['D','E','F'] => '3',
      (input) in ['G','H','I'] => '4',
      (input) in ['J','K','L'] => '5',
      (input) in ['M','N','O'] => '6',
      (input) in ['P','Q','R','S'] => '7',
      (input) in ['T','U','V'] => '8',
      (input) in ['W','X','Y','Z'] => '9',
            ''); //or you could default to return the input if anything else

outrec := record
  string letters;
    string numbers;
end;

outrec L2N(d L) := transform
  string12 TempNo := (L.number);
  self.letters := L.number;
  self.numbers := TranslateLetter(TempNo[1]) +
                  TranslateLetter(TempNo[2]) +
                  TranslateLetter(TempNo[3]) +
                  TranslateLetter(TempNo[4]) +
                  TranslateLetter(TempNo[5]) +
                  TranslateLetter(TempNo[6]) +
                  TranslateLetter2(TempNo[7]) +
                  TranslateLetter2(TempNo[8]) +
                  TranslateLetter2(TempNo[9]) +
                  TranslateLetter2(TempNo[10])+
                  TranslateLetter2(TempNo[11])+
                  TranslateLetter2(TempNo[12]);
end;
x := project(d,L2N(left));
output(x);
