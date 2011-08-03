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
