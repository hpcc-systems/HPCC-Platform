/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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

IMPORT mysql;

childrec := RECORD
   string name,
   integer value,
   boolean boolval,
   real8 r8,
   real4 r4,
   DATA d,
   DECIMAL10_2 ddd,
   UTF8 u1,
   UNICODE8 u2
END;

init := DATASET([{'name1', 1, true, 1.2, 3.4, D'aa55aa55', 1234567.89, U'Straße', U'Straße'},
                 {'name2', 2, false, 5.6, 7.8, D'00', -1234567.89, U'là', U'là'}], childrec);

drop() := EMBED(mysql : user('rchapman'),database('test'))
  DROP TABLE IF EXISTS tbl1;
ENDEMBED;

create() := EMBED(mysql : user('rchapman'),database('test'))
  CREATE TABLE tbl1 ( name VARCHAR(20), value INT, boolval TINYINT, r8 DOUBLE, r4 FLOAT, d BLOB, ddd DECIMAL(10,2), u1 VARCHAR(10), u2 VARCHAR(10) );
ENDEMBED;

initialize(dataset(childrec) values) := EMBED(mysql : user('rchapman'),database('test'))
  INSERT INTO tbl1 values (?, ?, ?, ?, ?, ?, ?, ?, ?);
ENDEMBED;

dataset(childrec) testMySQLDS() := EMBED(mysql : user('rchapman'),database('test'))
  SELECT * from tbl1;
ENDEMBED;

childrec testMySQLRow() := EMBED(mysql : user('rchapman'),database('test'))
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
   UNICODE8 u2) := EMBED(mysql : user('rchapman'),database('test'))
  SELECT * from tbl1 WHERE name=? AND value=? AND boolval=? AND r8=? AND r4=? AND d=? AND u1=? AND u2=?;
ENDEMBED;

string testMySQLString() := EMBED(mysql : user('rchapman'),database('test'))
  SELECT max(name) from tbl1;
ENDEMBED;

dataset(childrec) testMySQLStringParam(string filter) := EMBED(mysql : user('rchapman'),database('test'))
  SELECT * from tbl1 where name = ?;
ENDEMBED;

integer testMySQLInt() := EMBED(mysql : user('rchapman'),database('test'))
  SELECT max(value) from tbl1;
ENDEMBED;

boolean testMySQLBool() := EMBED(mysql : user('rchapman'),database('test'))
  SELECT max(boolval) from tbl1;
ENDEMBED;

real8 testMySQLReal8() := EMBED(mysql : user('rchapman'),database('test'))
  SELECT max(r8) from tbl1;
ENDEMBED;

real4 testMySQLReal4() := EMBED(mysql : user('rchapman'),database('test'))
  SELECT max(r4) from tbl1;
ENDEMBED;

data testMySQLData() := EMBED(mysql : user('rchapman'),database('test'))
  SELECT max(d) from tbl1;
ENDEMBED;

UTF8 testMySQLUtf8() := EMBED(mysql : user('rchapman'),database('test'))
  SELECT max(u1) from tbl1;
ENDEMBED;

UNICODE testMySQLUnicode() := EMBED(mysql : user('rchapman'),database('test'))
  SELECT max(u2) from tbl1;
ENDEMBED;

sequential (
  drop(),
  create(),
  initialize(init),
  OUTPUT(testMySQLDS()),
  OUTPUT(testMySQLRow().name),
  OUTPUT(testMySQLParms('name1', 1, true, 1.2, 3.4, D'aa55aa55', U'Straße', U'Straße')),
  OUTPUT(testMySQLString()),
  OUTPUT(testMySQLStringParam(testMySqlString())),
  OUTPUT(testMySQLInt()),
  OUTPUT(testMySQLBool()),
  OUTPUT(testMySQLReal8()),
  OUTPUT(testMySQLReal4()),
  OUTPUT(testMySQLData()),
  OUTPUT(testMySQLUtf8()),
  OUTPUT(testMySQLUnicode())
);