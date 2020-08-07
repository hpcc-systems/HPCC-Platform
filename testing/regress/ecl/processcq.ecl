/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

nested := RECORD
  STRING fname;
  INTEGER age;
  INTEGER income;
END;
 
Layout := RECORD
  INTEGER uid;
  DATASET(nested) info;
end;

inds := DATASET([
   {1,[{'FRED',21,1100},{'HELLEN',1,1400},{'JOHN',38,1300},{'JOHN',40,1200}]},
   {2,[{'FRED',10,2100},{'HELLEN',5,2400},{'JACK',30,2200},{'KATE',8,2300}]},
   {3,[{'TIM',35,3000}]}
  ],Layout);

groupedDs := GROUP(SORT(inds, uid), uid);

results := RECORD
  INTEGER UID;
  DATASET(nested) info;
  DATASET(nested) info1;
END;

results trans(inds r) := TRANSFORM
  sortedds := SORT(r.info, -income,-fname,-age);
  localrec := { nested payload, INTEGER recno };
  processformat := PROJECT(sortedds, TRANSFORM(localrec, SELF.recno := COUNTER, SELF.payload:=LEFT, SELF:=[]));

  nested processit(localrec rec, INTEGER c) := TRANSFORM
    p := PROCESS(processformat, rec, TRANSFORM(localrec, SELF:=RIGHT), TRANSFORM(localrec, SELF:=RIGHT));
    SELF := p[rec.recno].payload;
  END;
  SELF.info1 := PROJECT(processformat, processit(LEFT, COUNTER));    
  SELF := r;  
END;

Res0 := PROJECT(groupedDs, trans(LEFT));
Res0;
