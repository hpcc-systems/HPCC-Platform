/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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

//class=embedded
//class=3rdparty
//class=3rdpartyservice

IMPORT SqLite3;

//nothor
//Would need the database to be in a shared location for thor to work (and I don't know if sqlite is
//safe for concurrent access)
/*
 This example illustrates various calls to embdded SQLite code
 */

// This is the record structure in ECL that will correspond to the rows in the SQLite dataset
// Note that the default values specified in the fields will be used when a NULL value is being
// returned from SQLite

childrec := RECORD
   string name,
   integer4 value { default(99999) },
   boolean boolval { default(true) },
   real8 r8 {default(99.99)},
   real4 r4 {default(999.99)},
   DATA d {default (D'999999')},
   DECIMAL10_2 ddd {default(9.99)},
   UTF8 u1 {default(U'9999 ß')},
   UNICODE8 u2 {default(U'9999 ßßßß')}
END;

// Set up the SQLite database

drop() := EMBED(SqLite3 : file('test.db'))
  DROP TABLE IF EXISTS tbl1;
ENDEMBED;

create() := EMBED(SqLite3 : file('test.db'))
  CREATE TABLE tbl1 ( name VARCHAR(20), value INT, boolval TINYINT, r8 DOUBLE, r4 FLOAT, d BLOB, ddd DECIMAL(10,2), u1 VARCHAR(10), u2 VARCHAR(10) );
ENDEMBED;

// Add a row

initialize() := EMBED(SqLite3 : file('test.db'))
  INSERT INTO tbl1 values ('name1', 1, 1, 1.2, 3.4, 'aa55aa55', 1234567.89, 'Straße', 'Straße');
ENDEMBED;

// Add an additional row containing NULL values, to test that they are handled properly when read in ECL

initializeNulls() := EMBED(SqLite3 : file('test.db'))
  INSERT INTO tbl1 (name) values ('nulls');
ENDEMBED;

// Test various types of parameters

testSqLiteParms(
   string name,
   integer4 value,
   boolean boolval,
   real8 r8,
   real4 r4,
   DATA d,
   REAL8 ddd, // Decimal parms not supported.
   UTF8 u1,
   UNICODE8 u2) := EMBED(SqLite3 : file('test.db'))
  INSERT INTO tbl1 (name, value, boolval, r8, r4,d,ddd,u1,u2) values (:name, :value, :boolval, :r8, :r4,:d,:ddd,:u1,:u2);
ENDEMBED;

// Returning a dataset

dataset(childrec) testSQLiteDS() := EMBED(SqLite3 : file('test.db'))
  SELECT * from tbl1;
ENDEMBED;

// Returning a single row

childrec testSQLiteRow() := EMBED(SqLite3 : file('test.db'))
  SELECT * from tbl1 LIMIT 1;
ENDEMBED;

// Returning scalars

string testSQLiteString() := EMBED(SqLite3 : file('test.db'))
  SELECT max(name) from tbl1;
ENDEMBED;

dataset(childrec) testSQLiteStringParam(string filter) := EMBED(SqLite3 : file('test.db'))
  SELECT * from tbl1 where name = :filter;
ENDEMBED;

integer testSQLiteInt() := EMBED(SqLite3 : file('test.db'))
  SELECT max(value) from tbl1;
ENDEMBED;

boolean testSQLiteBool() := EMBED(SqLite3 : file('test.db'))
  SELECT max(boolval) from tbl1;
ENDEMBED;

real8 testSQLiteReal8() := EMBED(SqLite3 : file('test.db'))
  SELECT max(r8) from tbl1;
ENDEMBED;

real4 testSQLiteReal4() := EMBED(SqLite3 : file('test.db'))
  SELECT max(r4) from tbl1;
ENDEMBED;

data testSQLiteData() := EMBED(SqLite3 : file('test.db'))
  SELECT max(d) from tbl1;
ENDEMBED;

UTF8 testSQLiteUtf8() := EMBED(SqLite3 : file('test.db'))
  SELECT max(u1) from tbl1;
ENDEMBED;

UNICODE testSQLiteUnicode() := EMBED(SqLite3 : file('test.db'))
  SELECT max(u2) from tbl1;
ENDEMBED;

// Run the tests

sequential (
  drop(),
  create(),
  initialize(),
  initializeNulls(),
  testSQLiteParms('name2', 1, true, 1.2, 3.4, D'aa55aa55', 23.45, U'Straßaße', U'Straßßßße'),
  OUTPUT(testSQLiteDS()),
  OUTPUT(testSQLiteRow().name),
  OUTPUT(testSQLiteString()),
  OUTPUT(testSQLiteStringParam(testSQLiteString())),
  OUTPUT(testSQLiteInt()),
  OUTPUT(testSQLiteBool()),
  OUTPUT(testSQLiteReal8()),
  OUTPUT(testSQLiteReal4()),
  OUTPUT(testSQLiteData()),
  OUTPUT(testSQLiteUtf8()),
  OUTPUT(testSQLiteUnicode())
);
