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

IMPORT cassandra;

/*
 This example illustrates various calls to embdded Cassandra CQL code
 */

// This is the record structure in ECL that will correspond to the rows in the Cassandra dataset
// Note that the default values specified in the fields will be used when a NULL value is being
// returned from Cassandra

server := '127.0.0.1';

maprec := RECORD
   string fromVal => string toVal
 END;

childrec := RECORD
   string name,
   integer4 value { default(99999) },
   boolean boolval { default(true) },
   real8 r8 {default(99.99)},
   real4 r4 {default(999.99)},
   DATA d {default (D'999999')},
   DECIMAL10_2 ddd {default(9.99)},
   UTF8 u1 {default(U'9999 ß')},
   UNICODE u2 {default(U'9999 ßßßß')},
   STRING a,
   SET OF STRING set1,
   SET OF INTEGER4 list1,
   LINKCOUNTED DICTIONARY(maprec) map1{linkcounted};
END;

// Some data we will use to initialize the Cassandra table

init := DATASET([{'name1', 1, true, 1.2, 3.4, D'aa55aa55', 1234567.89, U'Straße', U'Straße','Ascii',['one','two','two','three'],[5,4,4,3],[{'a'=>'apple'},{'b'=>'banana'}]},
                 {'name2', 2, false, 5.6, 7.8, D'00', -1234567.89, U'là', U'là','Ascii', [],[],[]}], childrec);

init2 := ROW({'name4' , 3, true, 9.10, 11.12, D'aa55aa55', 987.65, U'Baße', U'Baße', '', [],[],[]}, childrec);

// Set up the Cassandra database
// Note that we can execute multiple statements in a single embed, provided that there are
// no parameters and no result type

// Note that server will default to localhost if not specified...

createks() := EMBED(cassandra : server(server),user('rchapman'))
  CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3' } ;
ENDEMBED;

createTables() := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'))
  DROP TABLE IF EXISTS tbl1;

  // Note that an ECL SET can map to either a SET or a LIST in Cassandra (it's actually closer to a LIST since repeated values are allowed and order is preserved)
  // When stored in a Cassandra SET field, duplicates will be discarded and order lost.
  // You can also use an ECL child dataset (with a single field) to map to a Cassandra SET or LIST.
  CREATE TABLE tbl1 ( name VARCHAR,
                      value INT,
                      boolval boolean,
                      r8 DOUBLE,
                      r4 FLOAT,
                      d BLOB,
                      ddd VARCHAR,
                      u1 VARCHAR,
                      u2 VARCHAR,
                      a ASCII,
                      set1 SET<varchar>,
                      list1 LIST<INT>,
                      map1 MAP<VARCHAR, VARCHAR>,
                      PRIMARY KEY (name) );
  CREATE INDEX tbl1_value  ON tbl1 (value);
  CREATE INDEX tbl1_boolval  ON tbl1 (boolval);
  INSERT INTO tbl1 (name, u1) values ('nulls', 'ß');  // Creates some null fields. Also note that query is utf8
ENDEMBED;

// Initialize the Cassandra table, passing in the ECL dataset to provide the rows
// When not using batch mode, maxFutures controls how many simultaenous writes to Cassandra are allowed before
// we start to throttle, and maxRetries controls how many times inserts that fail because Cassandra is too busy 
// will be retried.

initialize(dataset(childrec) values) := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'),maxFutures(100),maxRetries(10))
  INSERT INTO tbl1 (name, value, boolval, r8, r4,d,ddd,u1,u2,a,set1,list1,map1) values (?,?,?,?,?,?,?,?,?,?,?,?,?);
ENDEMBED;

// Note the batch option to control how cassandra inserts are batched
// If not supplied, each insert is executed individually - because Cassandra
// has restrictions about what can be done in a batch, we can't default to using batch
// unless told to... Also Cassandra 2.2 and later will fail if batch gets too large. In general
// best NOT to try to use batch for performance unless you know that 
//   (a) the resulting batch will be small (default limit is 50k bytes) and
//   (b) all the records in the batch share the same partition key and
//   (c) you use 'unlogged' mode
// Use of batch to ensure all trasactions either fail together of pass together is ok, but subject to the same size restrictions

initialize2(row(childrec) values) := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'),batch('unlogged'))
  INSERT INTO tbl1 (name, value, boolval, r8, r4,d,ddd,u1,u2,a,set1,list1,map1) values (?,?,?,?,?,?,?,?,?,?,?,?,?);
ENDEMBED;

// Returning a dataset

dataset(childrec) testCassandraDS() := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'))
  SELECT name, value, boolval, r8, r4,d,ddd,u1,u2,a,set1,list1,map1 from tbl1;
ENDEMBED;

// Returning a single row

childrec testCassandraRow() := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'))
  SELECT name, value, boolval, r8, r4,d,ddd,u1,u2,a,set1,list1,map1 from tbl1 LIMIT 1;
ENDEMBED;

// Passing in parameters

mapwrapper := RECORD
  LINKCOUNTED DICTIONARY(maprec) map1;
END;

testCassandraParms(
   string name,
   integer4 value,
   boolean boolval,
   real8 r8,
   real4 r4,
   DATA d,
//   DECIMAL10_2 ddd,
   UTF8 u1,
   UNICODE u2,
   STRING a,
   SET OF STRING set1,
   SET OF INTEGER4 list1,
   // Note we can't pass a dataset as a paramter to bind to a collection field - it would be interpreted as 'execute once per value in the dataset'
   // You have to pass a record containing the field as a child dataset
   ROW(mapwrapper) map1
   ) := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'))
  INSERT INTO tbl1 (name, value, boolval, r8, r4,d,ddd,u1,u2,a,set1,list1,map1) values (?,?,?,?,?,?,'8.76543',?,?,?,?,?,?);
ENDEMBED;

// Returning scalars

string testCassandraString() := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'))
  SELECT name from tbl1 LIMIT 1;
ENDEMBED;

dataset(childrec) testCassandraStringParam(string filter) := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'))
  SELECT name, value, boolval, r8, r4,d,ddd,u1,u2,a,set1,list1,map1 from tbl1 where name = ?;
ENDEMBED;

dataset(childrec) testCassandraStringSetParam(set of string filter) := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'))
  SELECT name, value, boolval, r8, r4,d,ddd,u1,u2,a,set1,list1,map1 from tbl1 where name IN ?;
ENDEMBED;

integer testCassandraInt() := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'))
  SELECT value from tbl1 LIMIT 1;
ENDEMBED;

boolean testCassandraBool() := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'))
  SELECT boolval from tbl1 WHERE name='name1';
ENDEMBED;

real8 testCassandraReal8() := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'))
  SELECT r8 from tbl1 WHERE name='name1';
ENDEMBED;

real4 testCassandraReal4() := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'))
  SELECT r4 from tbl1 WHERE name='name1';
ENDEMBED;

data testCassandraData() := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'))
  SELECT d from tbl1 WHERE name='name1';
ENDEMBED;

UTF8 testCassandraUtf8() := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'))
  SELECT u1 from tbl1 WHERE name='name1';
ENDEMBED;

UNICODE testCassandraUnicode() := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'))
  SELECT u2 from tbl1 WHERE name='name1';
ENDEMBED;

STRING testCassandraAscii() := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'))
  SELECT a from tbl1 WHERE name='name1';
ENDEMBED;

SET OF STRING testCassandraSet() := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'))
  SELECT set1 from tbl1 WHERE name='name1';
ENDEMBED;

SET OF INTEGER4 testCassandraList() := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'))
  SELECT list1 from tbl1 WHERE name='name1';
ENDEMBED;

// Just as you can't pass a dataset parameter to bind to a map column (only a child dataset of a record),
// if you wanted to return just a map column you have to do so via a child dataset

MapWrapper testCassandraMap() := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'))
  SELECT map1 from tbl1 WHERE name='name1';
ENDEMBED;

// Coding a TRANSFORM to call Cassandra - this ends up acting a little like a join

stringrec := RECORD
   string name
END;

TRANSFORM(childrec) t(stringrec L) := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'))
  SELECT name, value, boolval, r8, r4,d,ddd,u1,u2,a,set1,list1,map1 from tbl1 where name = ?;
ENDEMBED;

init3 := DATASET([{'name1'},
                  {'name2'}], stringrec);

testCassandraTransform := PROJECT(init3, t(LEFT));

// Passing in AND returning a dataset - this also ends up acting a bit like a keyed join...

stringrec extractName(childrec l) := TRANSFORM
  SELF := l;
END;

dataset(childrec) testCassandraDSParam(dataset(stringrec) inrecs) := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'))
  SELECT name, value, boolval, r8, r4,d,ddd,u1,u2,a,set1,list1,map1 from tbl1 where name = ?;
ENDEMBED;

// Testing performance of batch inserts

s1 :=DATASET(25000, TRANSFORM(childrec, SELF.name := 'name'+COUNTER,
                                         SELF.value:=COUNTER,
                                         SELF.boolval:=COUNTER % 2 =1,
                                         SELF.r8:=COUNTER*1.00001,
                                         SELF.r4:=COUNTER*1.001,
                                         SELF:=[]));

testCassandraBulk := initialize(s1);

// Check that 25000 got inserted

integer testCassandraCount() := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'))
  SELECT COUNT(*) from tbl1;
ENDEMBED;

dataset(childrec) testCassandraCountPaged(INTEGER ps) := FUNCTION
  dataset(childrec) r() := EMBED(cassandra : server(server),user('rchapman'),keyspace('test'),pageSize(ps))
    SELECT name, value, boolval, r8, r4,d,ddd,u1,u2,a,set1,list1,map1 from tbl1;
  ENDEMBED;
  return r();
END;

// Execute the tests

sequential (
  createks(),
  createTables(),
  initialize(init),

  testCassandraParms('name3', 1, true, 1.2, 3.4, D'aa55aa55', U'Straße', U'Straße', 'Only 7-bit US-ASCII chars allowed', ['four','five'], [2,2,3,1], ROW({[{'f'=>'fish'}]},MapWrapper)),
  initialize2(init2),
  OUTPUT(SORT(testCassandraDS(), name)),
  OUTPUT(testCassandraRow().name),
  OUTPUT(testCassandraString()),
  OUTPUT(testCassandraStringParam(testCassandraString())),
  OUTPUT(testCassandraStringSetParam(['name1', 'name2'])),
  OUTPUT(testCassandraInt()),
  OUTPUT(testCassandraBool()),
  OUTPUT(testCassandraReal8()),
  OUTPUT(testCassandraReal4()),
  OUTPUT(testCassandraData()),
  OUTPUT(testCassandraUtf8()),
  OUTPUT(testCassandraUnicode()),
  OUTPUT(testCassandraSet()),
  OUTPUT(testCassandraList()),
  OUTPUT(testCassandraMap().map1),
  OUTPUT(testCassandraTransform()),
  OUTPUT(testCassandraDSParam(PROJECT(init, extractName(LEFT)))),
  testCassandraBulk,
  OUTPUT(testCassandraCount()),
  OUTPUT(COUNT(testCassandraCountPaged(0))),
  OUTPUT(COUNT(testCassandraCountPaged(101))),
  OUTPUT(COUNT(testCassandraCountPaged(100000))),
  OUTPUT('Done');
);
