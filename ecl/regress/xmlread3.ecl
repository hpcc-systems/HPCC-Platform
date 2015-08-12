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

rec :=      RECORD,maxlength(99999)
string          month{xpath('/office:document-content/office:body/table:table/@table:name')};
unicode     body{xpath('<>')};
            END;


accounts := dataset('~file::127.0.0.1::temp::content.xml', rec, XML('/office:document-content/office:body/table:table/table:table-row'));

validAccounts := accounts(month != 'Cashflow' and month != 'Spending');


months := dedup(validAccounts, month);
output(months);
