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

