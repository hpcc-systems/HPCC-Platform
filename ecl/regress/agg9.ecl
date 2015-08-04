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

#option ('globalFold', false);

personRecord := RECORD
string4 house_id;
string20  forename;
string1   per_gender_code := '1';
string1 per_marital_status_code := '1';
    END;

personDataset := dataset([
        {'0002','Spiders','2'},
        {'0001','Gavin'},
        {'0002','Gavin'},
        {'0002','Mia'},
        {'0003','Extra'},
        {'0001','Mia'},
        {'0004','King'},
        {'0004','Queen'}], personRecord);


DIATRec := Record

  totalcount := SUM(GROUP, IF(TRUE, 1, 0));

  X000100__Gender__0000100_1_M_Male := SUM(GROUP,IF(personDataset.per_gender_code
IN ['1'], 1, 0));
  X000100__Gender__0000100_1_M_Male2 := COUNT(GROUP,personDataset.per_gender_code IN ['1']);
  X000100__Gender__0000200_2_3_4_F_Female := SUM(GROUP,IF
(personDataset.per_gender_code IN ['2','3'], 1, 0));
  X000100__Gender__0000200_2_3_4_F_Female2 := count(GROUP,personDataset.per_gender_code IN ['2','3']);

  X000200__Marital_Status__0000100__1_Maried := SUM(GROUP,IF
(personDataset.per_marital_status_code IN ['1'], 1, 0));
  X000200__Marital_Status__0000200_Not_Maried_Net := SUM(GROUP,IF
(personDataset.per_marital_status_code IN ['2','3','4','5'], 1, 0));

END;

tab := TABLE (personDataset, DIATRec);  // Currently works against some clusters

OUTPUT(tab,,'out.d00');
