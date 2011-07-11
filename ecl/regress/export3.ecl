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

LOADXML('<xml/>');

NamesRecord := record
string10 first;
string20 last;
        end;
r := RECORD
  unsigned integer4 dg_parentid;
  string10 dg_firstname;
  string dg_lastname;
  unsigned integer1 dg_prange;
  IFBLOCK(SELF.dg_prange % 2 = 0)
   string20 extrafield;
  END;
  NamesRecord nm;
  dataset(NamesRecord) names;
 END;

ds := dataset('ds', r, thor);


//Walk a record and do some processing on it.
#DECLARE(out)
#EXPORT(out, r);
LOADXML(%'out'%, 'FileStructure');

#FOR (FileStructure)
 #FOR (Field)
  #IF (%'{@isEnd}'% <> '')
output('END');
  #ELSE
output(%'@type'%
   #IF (%'@size'% <> '-15' AND %'@isRecord'%='' AND %'@isDataset'%='')
+ %'@size'%
   #end
 + ' ' + %'@label'% + ';');
  #END
 #END
#END

output('Done');


//This could be greatly simplified as (%'{IsAStringMetaInfo/Field[1]/@type}'%='string')
isAString(inputField) := macro
#EXPORTXML(IsAStringMetaInfo, inputField)
#IF (%'IsAString'%='')
 #DECLARE(IsAString)
#END
#SET(IsAString, false)
#FOR (IsAStringMetaInfo)
 #FOR (Field)
  #IF (%'@type'% = 'string')
   #SET (IsAString, true)
  #END
  #BREAK
 #END
#END
%IsAString%
endmacro;


getFieldName(inputField) := macro
#EXPORTXML(GetFieldNameMetaInfo, inputField)
%'{GetFieldNameMetaInfo/Field[1]/@name}'%
endmacro;


displayIsAString(inputField) := macro
output(getFieldName(inputField) + trim(IF(isAString(inputField), ' is', ' is not')) + ' a string.')
endmacro;

sizeof(r.dg_firstname);
isAString(r.dg_firstname);
getFieldName(r.dg_firstname);
output('ds.dg_firstname isAString? ' + (string)isAString(ds.dg_firstname));
isAString(ds.nm);
sizeof(ds.nm);

displayIsAString(ds.nm);
displayIsAString(r.dg_firstname);
