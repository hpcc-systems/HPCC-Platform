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

InRec := RECORD
    string UIDstr;
    string LeftInStr;
    string RightInStr;
END;
InDS := DATASET([
{'1','the quick brown fox jumped over the lazy red dog','quick fox red dog'},
{'2','the quick brown fox jumped over the lazy red dog','quick fox black dog'},
{'3','george of the jungle lives here','fox black dog'},
{'4','fred and wilma flintstone','fred flintstone'},
{'5','osama obama yomama comeonah','barak hillary'}
],InRec);
output(InDS,,'RTTEST::TEST::CSVHeaderTest',CSV(HEADING(SINGLE)),overwrite);

