/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

//class=file
//version multiPart=true

#onwarning(7102, ignore);

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
useLocal := #IFDEFINED(root.useLocal, false);
useTranslation := #IFDEFINED(root.useTranslation, false);

//--- end of version configuration ---

import $.setup;
Files := setup.Files(multiPart, useLocal, useTranslation);

prefix := setup.Files(false, false).QueryFilePrefix;

tableColumn := RECORD
 string style{xpath('@table:style-name')};
 string repeated{xpath('table:number-columns-repeated')};
 string defaultCellStyle{xpath('@table:default-cell-style-name')};
END;

tableRow := RECORD,maxlength(99999)
 string          month{xpath('/office:document-content/office:body/table:table/@table:name')};
 varstring       date{xpath('table:table-cell[1]/text:p')};
 varunicode      description{xpath('table:table-cell[2]/text:p')};
 varunicode      fullDescription{xpath('table:table-cell[2]/text:p<>')};
 varstring       amount{xpath('table:table-cell[3]/text:p')};
END;

tableTableWithoutNestedDS := RECORD
 tableColumn col1{xpath('table:table-column[1]')};
 tableColumn col2{xpath('table:table-column[2]')};
 tableColumn col3{xpath('table:table-column[3]')};
 tableColumn col4{xpath('table:table-column[4]')};
END;

tableTable := RECORD(tableTableWithoutNestedDS)
 DATASET(tableRow) rows{xpath('table:table-row')};
END;

accounts := dataset(Files.DG_FileOut+'accountxml', tableTable, XML('/office:document-content/office:body/table:table'));

newTableColumn := RECORD
 string defaultCellStyle{xpath('@table:default-cell-style-name')};
 string style{xpath('@table:style-name')};
END;

newTableRow := RECORD
 varunicode      description;
 varunicode      fullDescription;
 string          date;
 real8           amount;
END;

newTableTableWithoutNestedDS := RECORD
 newTableColumn col3;
 newTableColumn col1;
 newTableColumn col4;
END;

newTableTable := RECORD(newTableTableWithoutNestedDS)
 DATASET(newTableRow) rows{xpath('table:table-row')};
END;

newaccountsflat := dataset(prefix+'accountsflat', newTableTable, FLAT);
newaccountscsv := dataset(prefix+'accountscsv', newTableTableWithoutNestedDS, CSV);
newaccountsxml := dataset(prefix+'accountsxml', newTableTable, XML('/Dataset/Row'));
newaccountsjson := dataset(prefix+'accountsjson', newTableTable, JSON('/Row'));

SEQUENTIAL(
 PARALLEL(
  OUTPUT(accounts, , prefix+'accountsflat', OVERWRITE);
  OUTPUT(accounts, , prefix+'accountscsv', CSV, OVERWRITE);
  OUTPUT(accounts, , prefix+'accountsxml', XML, OVERWRITE);
  OUTPUT(accounts, , prefix+'accountsjson', JSON, OVERWRITE);
 );
 OUTPUT(newaccountsflat, NAMED('newaccountsflat'));
 OUTPUT(newaccountscsv, NAMED('newaccountscsv'));
 OUTPUT(newaccountsxml, NAMED('newaccountsxml'));
 OUTPUT(newaccountsjson, NAMED('newaccountsjson'));
);
