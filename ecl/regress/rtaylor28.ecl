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

