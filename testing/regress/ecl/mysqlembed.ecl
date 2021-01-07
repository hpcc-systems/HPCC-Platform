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
//class=3rdpartyservice

IMPORT mysql;

import ^ as root;
myServer := #IFDEFINED(root.mysqlserver, 'localhost');
myUser := #IFDEFINED(root.mysqluser, 'rchapman');
myDb := #IFDEFINED(root.mysqldb, 'test');

stringrec := RECORD
   string name
END;

childrec := RECORD(stringrec)
   integer2 bval {default(-1)},
   integer4 value { default(99999) },
   boolean boolval { default(true) },
   real8 r8 {default(99.99)},
   real4 r4 {default(999.99)},
   DATA d {default (D'999999')},
   DECIMAL10_2 ddd {default(9.99)},
   UTF8 u1 {default(U'9999 ß')},
   record
     UNICODE8 u2 {default(U'9999 ßßßß')},
   end;
   STRING19 dt {default('1963-11-22 12:30:00')},
END;


stringrec extractName(childrec l) := TRANSFORM
  SELF := l;
END;

init := DATASET([{'name1', 0x4161, 1, true, 1.2, 3.4, D'aa55aa55', 1234567.89, U'Straße', U'Straße'},
                 {'name2', 66, 2, false, 5.6, 7.8, D'00', -1234567.89, U'là', U'là', '2015-12-25 01:23:45' }], childrec);

drop1() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  DROP TABLE IF EXISTS tbl1;
ENDEMBED;

drop2() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  DROP PROCEDURE IF EXISTS testSP;
ENDEMBED;

drop3() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  DROP FUNCTION IF EXISTS hello;
ENDEMBED;

drop() := SEQUENTIAL(drop1(), drop2(), drop3());

create1() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  CREATE TABLE tbl1 ( name VARCHAR(20), bval BIT(15), value INT, boolval TINYINT, r8 DOUBLE, r4 FLOAT, d BLOB, ddd DECIMAL(10,2), u1 VARCHAR(10), u2 VARCHAR(10), dt DATETIME );
ENDEMBED;

create2() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  CREATE FUNCTION hello(s CHAR(255)) RETURNS char(255) DETERMINISTIC
     RETURN CONCAT('Hello, ',s,'!') 
ENDEMBED;

create3() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  CREATE PROCEDURE testSP(IN in_name VARCHAR(255))
  BEGIN
     SELECT * FROM tbl1 where name=in_name;
  END
ENDEMBED;

create() := SEQUENTIAL(create1(), create2(), create3());

initialize(dataset(childrec) values) := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  INSERT INTO tbl1 values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
ENDEMBED;

initializeNulls() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  INSERT INTO tbl1 (name) values ('nulls');
ENDEMBED;

initializeUtf8() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  INSERT INTO tbl1 values ('utf8test', 65, 1, 1, 1.2, 3.4, 'aa55aa55', 1234567.89, 'Straße', 'Straße', '2019-02-01 23:59:59');
ENDEMBED;

dataset(childrec) testMySQLDS() := EMBED(mysql : server(myServer),user(myUser),database(myDB),PROJECTED('OUTPUTFIELDS'))
  SELECT OUTPUTFIELDS from tbl1;
ENDEMBED;

dataset(childrec) testMySQLDS2() := EMBED(mysql : server(myServer),user(myUser),database(myDB),PROJECTED('[]'))
  SELECT [] from tbl1 where u1='Straße';
ENDEMBED;

// NOTE - while this works as a test case you really don't want to be using the modulo in this way to pull a dataset in parallel, as it won't be able to be indexed efficiently in MySQL

streamed dataset(childrec) testMySQLDS2a() := EMBED(mysql : server(myServer),user(myUser),database(myDB),PROJECTED('[]'), activity, local(false))
  SELECT [] from tbl1 where (value is NULL and __activity__.slave=0) OR (value % __activity__.numSlaves = __activity__.slave);
ENDEMBED;

ds3query := u'SELECT ** FROM tbl1;' : STORED('ds3query');

dataset(childrec) testMySQLDS3() := EMBED(mysql, ds3query : server(myServer),user(myUser),database(myDB),PROJECTED(u'**'));

childrec testMySQLRow() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  SELECT * from tbl1 LIMIT 1;
ENDEMBED;

TRANSFORM(childrec) mysqlTransform(string name) := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  SELECT * from tbl1 WHERE name=? LIMIT 1;
ENDEMBED;

testMySQLTransform() := PROJECT(init, mySQLTransform(LEFT.name));

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

string testMySQLStoredProcedure() := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  select Hello('World');
ENDEMBED;

streamed dataset(childrec) testMySQLStoredProcedure2(STRING lid) := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  CALL testSP(?);
ENDEMBED;

streamed dataset(childrec) testMySQLStoredProcedure3(DATASET(stringrec) lids) := EMBED(mysql : server(myServer),user(myUser),database(myDB))
  CALL testSP(?);
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
      COUNT(testMySQLDS2()),
      OUTPUT(SORT(testMySQLDS2a(), name)),
      OUTPUT(testMySQLDS3(), {name}),
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
      OUTPUT(testMySQLDateTime()),
      OUTPUT(testMySQLTransform()),
      OUTPUT(testMySQLStoredProcedure()),
      OUTPUT(testMySQLStoredProcedure2('name1')),
      OUTPUT(testMySQLStoredProcedure3(DATASET([{'name1'},{'name2'}], stringrec)))
  )
);
