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

EXPORT serialTest := MODULE

EXPORT wordRec := { string word; };

//A DATASET nested two levels deep
EXPORT bookDsRec := RECORD
  string title;
  DATASET(wordRec) words;
END;

EXPORT libraryDsRec := RECORD
  string owner;
  DATASET(bookDsRec) books;
END;


// Same for a DICTIONARY
EXPORT bookDictRec := RECORD
  string title
  =>
  DICTIONARY(wordRec) words;
END;

EXPORT libraryDictRec := RECORD
  string owner
  =>
  DICTIONARY(bookDictRec) books;
END;

EXPORT DsFilename := 'REGRESS::SerialLibraryDs';

EXPORT DictFilename := 'REGRESS::SerialLibraryDict';

EXPORT DictKeyFilename := 'REGRESS::SerialLibraryKeyDict';

EXPORT BookKeyFilename := 'REGRESS::SerialBookKey';

EXPORT libraryDictionaryFile := DATASET(DictFilename, LibraryDictRec, THOR);

EXPORT libraryDatasetFile := DATASET(DsFilename, LibraryDsRec, THOR);

EXPORT bookIndex := INDEX({ string20 title }, { dataset(wordRec) words }, BookKeyFilename);

END; /* serial */