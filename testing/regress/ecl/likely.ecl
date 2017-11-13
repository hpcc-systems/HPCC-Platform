/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

//nohthor
//noroxie
import $.setup;
prefix := setup.Files(false, false).FilePrefix;

PeopleFileName := prefix + 'people_likely';

// GENERATE People Dataset
Layout_Person := RECORD
  UNSIGNED1 PersonID;
  STRING15  FirstName;
  STRING25  LastName;
  UNSIGNED  Age;
  STRING25  City;
END;

ds0 := DATASET([ {  1, 'TIMTOHY',  'PRUNER',    24, 'ABBEVILE'},
                 {  2, 'ALCIAN',   'STRACK',    33, 'AGOURA HILLS'},
                 {  3, 'CHAMENE',  'TRAYLOR',   44, 'ABELL'},
                 {  4, 'HATIM',    'BARENT',    62, 'AIEA'},
                 {  5, 'MIRCHINE', 'CECCHETTI', 22, 'ANDOVER'},
                 {  6, 'RENORDA',  'HEERMANN',  19, 'ABERDEEN'},
                 {  7, 'NAIR',     'KREDA',     29, 'ANDREWS'},
                 {  8, 'RYLE',     'REDMAN',    61, 'AKROW'},
                 {  9, 'NAATALIE', 'VANSPANJE', 29, 'ANDALE'},
                 { 10, 'MINGWAI',  'MINTE',     35, 'ABILENE'},
                 { 11, 'RITUA',    'CORLETO',   42, 'ANCHORAGE'} ], Layout_Person);

SetupPeople := OUTPUT(ds0,,PeopleFileName, OVERWRITE);

// Test Likely/Unlikely
PeopleDS1 := dataset(PeopleFileName, Layout_Person, thor);

// Spill
filter0 := PeopleDS1( LIKELY(FirstName <> '', 0.01) );
filter0_1 := filter0( LIKELY(City <>'', 0.01) );
filter0_2 := filter0( LIKELY(Age < 99, 0.01));
filter0_1_1 := sort(filter0_1( LIKELY(LastName<>'xxx', 0.01)), LastName );
filter0_2_1 := sort(filter0_2( LIKELY(LastName<>'yyy', 0.01)), FirstName );

// Expand rather than spill
filter1 := PeopleDS1( LIKELY(FirstName <> '', 0.99) );
filter1_1 := filter1( LIKELY(City <>'', 0.99) );
filter1_2 := filter1( LIKELY(Age < 99, 0.99));
filter1_1_1 := sort(filter1_1( LIKELY(LastName<>'xxx', 0.99)), LastName );
filter1_2_1 := sort(filter1_2( LIKELY(LastName<>'yyy', 0.99)), FirstName );

// Spill
filter2 := PeopleDS1( LIKELY(FirstName <> '', 0.9) );
filter2_1 := filter2( LIKELY(City <>'', 0.4) );
filter2_2 := filter2( LIKELY(Age < 99, 0.4));
filter2_1_1 := sort(filter2_1( LIKELY(LastName<>'xxx', 0.1)), LastName );
filter2_1_2 := sort(filter2_1( LIKELY(LastName<>'abc', 0.1)), LastName );
filter2_2_1 := sort(filter2_2( LIKELY(LastName<>'yyy', 0.1)), FirstName );
filter2_2_2 := sort(filter2_2( LIKELY(LastName<>'def', 0.1)), FirstName );

// Expand rather than spill
filter3 := PeopleDS1( LIKELY(FirstName <> '', 0.9), HINT(gofaster(true)) );
filter3_1 := filter3( LIKELY(City <>'', 0.4) );
filter3_2 := filter3( LIKELY(Age < 99, 0.4));
filter3_1_1 := sort(filter3_1( LIKELY(LastName<>'xxx', 0.1)), LastName );
filter3_1_2 := sort(filter3_1( LIKELY(LastName<>'abc', 0.1)), LastName );
filter3_2_1 := sort(filter3_2( LIKELY(LastName<>'yyy', 0.1)), FirstName );
filter3_2_2 := sort(filter3_2( LIKELY(LastName<>'def', 0.1)), FirstName );
filter3_2_2_1 := filter3_2_2( LIKELY(Age <> 24,0.1234), HINT(maxnumber(110)) );
filter3_2_2_2 := filter3_2_2( LIKELY(Age <> 4,0.01) );
filter3_2_2_1_1 := filter3_2_2_1( LIKELY(PersonId <> 34,0.01) );
filter3_2_2_1_2 := filter3_2_2_1( LIKELY(PersonId <> 23,0.01) );

SEQUENTIAL(
  SetupPeople,
  PARALLEL(
    OUTPUT(filter0_1_1);
    OUTPUT(filter0_2_1);
  ),
  PARALLEL(
    OUTPUT(filter1_1_1);
    OUTPUT(filter1_2_1);
  ),
  PARALLEL(
    OUTPUT(filter2_1_1);
    OUTPUT(filter2_1_2);
    OUTPUT(filter2_2_1);
    OUTPUT(filter2_2_2);
  ),
  PARALLEL(
    OUTPUT(filter3_2_1);
    OUTPUT(filter3_2_2);
    OUTPUT(filter3_2_2_1_1);
    OUTPUT(filter3_2_2_1_2);
  )
);

