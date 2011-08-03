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
