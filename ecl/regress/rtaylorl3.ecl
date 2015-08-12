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

NamesRec := RECORD
    INTEGER1  NameID;
    STRING20  FName;
    STRING20  LName;
END;

NamesTable := DATASET([ {1,'Kevin','Holliday'},
                        {2,'Mia','Taylor'},
                                                {3,'Mr','Nobody'},
                                                {4,'Anywhere','but here'}],
                                            NamesRec);

// FilterLibraryInterface(dataset(namesRec) ds, string search) := INTERFACE
    // EXPORT dataset(namesRec) matches;
    // EXPORT dataset(namesRec) others;
// END;

// FilterDatasetLibrary(dataset(namesRec) ds, string search) := MODULE,LIBRARY(FilterLibraryInterface)
    // export matches := ds(Lname = search);
    // export others := ds(Lname != search);
// end;


// result := LIBRARY(INTERNAL(FilterDatasetLibrary), FilterLibraryInterface(NamesTable, 'Holliday'));
// result := LIBRARY('FilterDatasetLibrary', FilterLibraryInterface(NamesTable, 'Holliday'));


IFilterArgs := INTERFACE
  export dataset(namesRec) ds;
  export string search;
END;

FilterLibraryInterface(IFilterArgs args) := INTERFACE
    EXPORT dataset(namesRec) matches;
    EXPORT dataset(namesRec) others;
END;

FilterDatasetLibrary(IFilterArgs args) := MODULE,LIBRARY(FilterLibraryInterface)
    export matches := args.ds(Lname = args.search);
    export others := args.ds(Lname != args.search);
end;

// SearchArgs := MODULE(IFilterArgs)
  // export dataset(namesRec) ds := NamesTable;
  // export string search := 'Holliday';
// end;
// result := LIBRARY(INTERNAL(FilterDatasetLibrary), FilterLibraryInterface(SearchArgs));
// result := LIBRARY('FilterDatasetLibrary', FilterLibraryInterface(SearchArgs));

#workunit('name','FilterDatasetLibrary');
BUILD(FilterDatasetLibrary);

// OUTPUT(result.matches);
// OUTPUT(COUNT(result.others));

