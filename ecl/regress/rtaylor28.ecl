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

SetStuff := ['JUNK', 'GARBAGE', 'CRAP'];

ds := DATASET(SetStuff,{string10 word});

ds2 := DATASET([{1,'FRED'},
              {2,'GEORGE'},
              {3,'CRAPOLA'},
              {4,'JUNKER'},
              {5,'GARBAGEGUY'},
              {6,'FREDDY'},
              {7,'TIM'},
              {8,'JOHN'},
              {9,'MIKE'}
             ],{unsigned6 ID,string10 firstname});

outrec := record
  ds2.ID;
            ds2.firstname;
            boolean GotCrap;
end;

{boolean GotCrap} XF2(ds L, STRING10 inword)    := TRANSFORM
  SELF.GotCrap := IF(StringLib.StringFind(inword,L.word,1)>0,TRUE,SKIP);
END;

outrec XF1(ds2 L)           := TRANSFORM
  SELF.GotCrap := EXISTS(PROJECT(ds,XF2(LEFT,L.firstname)));
            self := L;
END;

stuff := PROJECT(ds2,XF1(LEFT));

OUTPUT(ds);
OUTPUT(ds2);
OUTPUT(Stuff);

  // StringLib.StringFind()

