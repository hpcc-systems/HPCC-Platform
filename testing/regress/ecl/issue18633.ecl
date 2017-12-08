/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

NotLength(STRING s) := 'Length was not in:'+s;

Layout_out := RECORD
      UNSIGNED FieldsChecked_WithErrors;
      UNSIGNED FieldsChecked_NoErrors;
      UNSIGNED Fields_dummyfield_LengthErrors;
END;

dsTest := DATASET([{2, 0, 1}], Layout_out);


ScrubsOrbitLayout := RECORD
  STRING    ErrorMessage;
  UNSIGNED8 rulecnt;
END;

ScrubsOrbitLayout xNorm(dsTest L, INTEGER c) := TRANSFORM
      SELF.ErrorMessage := CHOOSE(c, 'Fields with errors', 'Fields without errors', NotLength('1..'));
      SELF.rulecnt := CHOOSE(c, L.FieldsChecked_WithErrors, L.FieldsChecked_NoErrors, L.Fields_dummyfield_LengthErrors);
END;

dsScrubs := NORMALIZE(dsTest, 3, xNorm(LEFT, COUNTER));

OUTPUT(dsScrubs);

z(string x) := '?' + x;

dsTest2 := DATASET([2, 0, 1, 3, 4, 7], { unsigned id});

{ string msg } xNorm2(INTEGER c) := TRANSFORM
      SELF.msg := CASE(c, 1=>'One', 2=>'Two', 4=>'Four', 5=>'Five', z('?'));
END;

dsScrubs2 := PROJECT(nofold(dsTest2), xNorm2(LEFT.id));
OUTPUT(dsScrubs2);
