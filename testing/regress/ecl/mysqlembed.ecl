/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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

IMPORT mysql;

myServer := 'localhost' : stored('myServer');
myUser := 'rchapman' : stored('myUser');
myDb := 'test' : stored('myDb');

childrec := RECORD
   string name,
   integer4 value { default(99999) },
   boolean boolval { default(true) },
   real8 r8 {default(99.99)},
   real4 r4 {default(999.99)},
   DATA d {default (D'999999')},
   DECIMAL10_2 ddd {default(9.99)},
   UTF8 u1 {default(U'9999 ß')},
   UNICODE8 u2 {default(U'9999 ßßßß')},
   STRING19 dt {default('1963-11-22 12:30:00')},
END;

stringrec := RECORD
   string name
END;

stringrec extractName(childrec l) := TRANSFORM
  SELF := l;
END;

init := DATASET([{'name1', 1, true, 1.2, 3.4, D'aa55aa55', 1234567.89, U'Straße', U'Straße'},
                 {'name2', 2, false, 5.6, 7.8, D'00', -1234567.89, U'là', U'là', '2015-12-25 01:23:45' }], childrec);

drop() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  DROP TABLE IF EXISTS tbl1;
ENDEMBED;

create() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  CREATE TABLE tbl1 ( name VARCHAR(20), value INT, boolval TINYINT, r8 DOUBLE, r4 FLOAT, d BLOB, ddd DECIMAL(10,2), u1 VARCHAR(10), u2 VARCHAR(10), dt DATETIME );
ENDEMBED;

initialize(dataset(childrec) values) := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  INSERT INTO tbl1 values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
ENDEMBED;

initializeNulls() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  INSERT INTO tbl1 (name) values ('nulls');
ENDEMBED;

initializeUtf8() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  INSERT INTO tbl1 values ('utf8test', 1, 1, 1.2, 3.4, 'aa55aa55', 1234567.89, 'Straße', 'Straße', '2019-02-01 23:59:59');
ENDEMBED;

dataset(childrec) testMySQLDS() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  SELECT * from tbl1;
ENDEMBED;

childrec testMySQLRow() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  SELECT * from tbl1 LIMIT 1;
ENDEMBED;

childrec testMySQLParms(
   string name,
   integer value,
   boolean boolval,
   real8 r8,
   real4 r4,
   DATA d,
   UTF8 u1,
   UNICODE8 u2) := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  SELECT * from tbl1 WHERE name=? AND value=? AND boolval=? AND r8=? AND r4=? AND d=? AND u1=? AND u2=?;
ENDEMBED;

string testMySQLString() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  SELECT max(name) from tbl1;
ENDEMBED;

dataset(childrec) testMySQLStringParam(string filter) := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  SELECT * from tbl1 where name = ?;
ENDEMBED;

dataset(childrec) testMySQLDSParam(dataset(stringrec) inrecs) := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  SELECT * from tbl1 where name = ?;
ENDEMBED;

integer testMySQLInt() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  SELECT max(value) from tbl1;
ENDEMBED;

boolean testMySQLBool() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  SELECT max(boolval) from tbl1;
ENDEMBED;

real8 testMySQLReal8() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  SELECT max(r8) from tbl1;
ENDEMBED;

real4 testMySQLReal4() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  SELECT max(r4) from tbl1;
ENDEMBED;

data testMySQLData() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  SELECT max(d) from tbl1;
ENDEMBED;

UTF8 testMySQLUtf8() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  SELECT max(u1) from tbl1;
ENDEMBED;

UNICODE testMySQLUnicode() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  SELECT max(u2) from tbl1;
ENDEMBED;

datetimerec := RECORD
   UNSIGNED8 dt1;
   STRING19 dt2;
END;

dataset(datetimerec) testMySQLDateTime() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  SELECT dt, dt from tbl1;
ENDEMBED;


sequential (
  drop(),
  create(),
  initialize(init),
  initializeNulls(),
  initializeUtf8(),
  PARALLEL (
  OUTPUT(testMySQLDS()),
  OUTPUT(testMySQLRow().name),
  OUTPUT(testMySQLParms('name1', 1, true, 1.2, 3.4, D'aa55aa55', U'Straße', U'Straße')),
  OUTPUT(testMySQLString()),
  OUTPUT(testMySQLStringParam(testMySqlString())),
  OUTPUT(testMySQLDSParam(PROJECT(init, extractName(LEFT)))),
    OUTPUT(testMySQLInt()+testMySQLInt()),
  OUTPUT(testMySQLBool()),
  OUTPUT(testMySQLReal8()),
  OUTPUT(testMySQLReal4()),
  OUTPUT(testMySQLData()),
  OUTPUT(testMySQLUtf8()),
  OUTPUT(testMySQLUnicode()),
  OUTPUT(testMySQLDateTime())
  )
);
