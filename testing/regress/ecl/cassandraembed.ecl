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

IMPORT cassandra;

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

stringrec := RECORD
   string name
END;

stringrec extractName(childrec l) := TRANSFORM
  SELF := l;
END;

init := DATASET([{'name1' , 1, true, 1.2, 3.4, D'aa55aa55', 1234567.89, U'Straße', U'Straße'},
                 {'name2', 2, false, 5.6, 7.8, D'00', -1234567.89, U'là', U'là'}], childrec);

init2 := ROW({'name4' , 3, true, 9.10, 11.12, D'aa55aa55', 987.65, U'Baße', U'Baße'}, childrec);

createks() := EMBED(cassandra : user('rchapman'))
  CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3' } ;
ENDEMBED;

createTables() := EMBED(cassandra : user('rchapman'),keyspace('test'))
  DROP TABLE IF EXISTS tbl1;

  CREATE TABLE tbl1 ( name VARCHAR, value INT, boolval boolean , r8 DOUBLE, r4 FLOAT, d BLOB, ddd VARCHAR, u1 VARCHAR, u2 VARCHAR,   PRIMARY KEY (name) );
  CREATE INDEX tbl1_value  ON tbl1 (value);
  CREATE INDEX tbl1_boolval  ON tbl1 (boolval);
  INSERT INTO tbl1 (name,u1) values ('; /* // ?', 'Straßßßßßße');
ENDEMBED;

initialize(dataset(childrec) values) := EMBED(cassandra : user('rchapman'),keyspace('test'),batch('unlogged'))
  INSERT INTO tbl1 (name, value, boolval, r8, r4,d,ddd,u1,u2) values (?,?,?,?,?,?,?,?,?);
ENDEMBED;

initialize2(row(childrec) values) := EMBED(cassandra : user('rchapman'),keyspace('test'))
  INSERT INTO tbl1 (name, value, boolval, r8, r4,d,ddd,u1,u2) values (?,?,?,?,?,?,?,?,?);
ENDEMBED;

dataset(childrec) testCassandraDS() := EMBED(cassandra : user('rchapman'),keyspace('test'))
  SELECT name, value, boolval, r8, r4,d,ddd,u1,u2 from tbl1;
ENDEMBED;

childrec testCassandraRow() := EMBED(cassandra : user('rchapman'),keyspace('test'))
  SELECT name, value, boolval, r8, r4,d,ddd,u1,u2 from tbl1 LIMIT 1;
ENDEMBED;

testCassandraParms(
   string name,
   integer4 value,
   boolean boolval,
   real8 r8,
   real4 r4,
   DATA d,
//   DECIMAL10_2 ddd,
   UTF8 u1,
   UNICODE8 u2) := EMBED(cassandra : user('rchapman'),keyspace('test'))
  INSERT INTO tbl1 (name, value, boolval, r8, r4,d,ddd,u1,u2) values (?,?,?,?,?,?,'8.76543',?,?);
ENDEMBED;

string testCassandraString() := EMBED(cassandra : user('rchapman'),keyspace('test'))
  SELECT name from tbl1 LIMIT 1;
ENDEMBED;

dataset(childrec) testCassandraStringParam(string filter) := EMBED(cassandra : user('rchapman'),keyspace('test'))
  SELECT name, value, boolval, r8, r4,d,ddd,u1,u2 from tbl1 where name = ?;
ENDEMBED;

dataset(childrec) testCassandraDSParam(dataset(stringrec) inrecs) := EMBED(cassandra : user('rchapman'),keyspace('test'))
  SELECT name, value, boolval, r8, r4,d,ddd,u1,u2 from tbl1 where name = ?;
ENDEMBED;

integer testCassandraInt() := EMBED(cassandra : user('rchapman'),keyspace('test'))
  SELECT value from tbl1 LIMIT 1;
ENDEMBED;

boolean testCassandraBool() := EMBED(cassandra : user('rchapman'),keyspace('test'))
  SELECT boolval from tbl1 WHERE name='name1';
ENDEMBED;

real8 testCassandraReal8() := EMBED(cassandra : user('rchapman'),keyspace('test'))
  SELECT r8 from tbl1 WHERE name='name1';
ENDEMBED;

real4 testCassandraReal4() := EMBED(cassandra : user('rchapman'),keyspace('test'))
  SELECT r4 from tbl1 WHERE name='name1';
ENDEMBED;

data testCassandraData() := EMBED(cassandra : user('rchapman'),keyspace('test'))
  SELECT d from tbl1 WHERE name='name1';
ENDEMBED;

UTF8 testCassandraUtf8() := EMBED(cassandra : user('rchapman'),keyspace('test'))
  SELECT u1 from tbl1 WHERE name='name1';
ENDEMBED;

UNICODE testCassandraUnicode() := EMBED(cassandra : user('rchapman'),keyspace('test'))
  SELECT u2 from tbl1 WHERE name='name1';
ENDEMBED;

searchrec := RECORD
  STRING name;
END;

TRANSFORM(childrec) t(searchrec L) := EMBED(cassandra : user('rchapman'),keyspace('test'))
  SELECT name, value, boolval, r8, r4,d,ddd,u1,u2 from tbl1 where name = ?;
ENDEMBED;

integer testCassandraCount() := EMBED(cassandra : user('rchapman'),keyspace('test'))
  SELECT COUNT(*) from tbl1;
ENDEMBED;

init3 := DATASET([{'name1'},
                  {'name2'}], searchrec);

testCassandraTransform := PROJECT(init3, t(LEFT));

s1 :=DATASET(25000, TRANSFORM(childrec, SELF.name := 'name'+COUNTER,
                                         SELF.value:=COUNTER,
                                         SELF.boolval:=COUNTER % 2 =1,
                                         SELF.r8:=COUNTER*1.00001,
                                         SELF.r4:=COUNTER*1.001,
                                         SELF:=[]));

testCassandraBulk := initialize(s1);

sequential (
  createks(),
  createTables(),
  initialize(init),

  testCassandraParms('name3', 1, true, 1.2, 3.4, D'aa55aa55', U'Straße', U'Straße'),
  initialize2(init2),
  OUTPUT(SORT(testCassandraDS(), name)),
  OUTPUT(testCassandraRow().name),
  OUTPUT(testCassandraString()),
  OUTPUT(testCassandraStringParam(testCassandraString())),
  OUTPUT(testCassandraDSParam(PROJECT(init, extractName(LEFT)))),
  OUTPUT(testCassandraInt()),
  OUTPUT(testCassandraBool()),
  OUTPUT(testCassandraReal8()),
  OUTPUT(testCassandraReal4()),
  OUTPUT(testCassandraData()),
  OUTPUT(testCassandraUtf8()),
  OUTPUT(testCassandraUnicode()),
  OUTPUT(testCassandraTransform()),
  testCassandraBulk,
  OUTPUT(testCassandraCount())
);