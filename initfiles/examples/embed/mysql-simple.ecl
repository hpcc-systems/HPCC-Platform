IMPORT mysql;

/*
 This example illustrates various calls to embdded MySQL code
 */

// This is the record structure in ECL that will correspond to the rows in the MySQL dataset

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

// Some data we will use to initialize the MySQL table

init := DATASET([{'name1', 1, true, 1.2, 3.4, D'aa55aa55', 1234567.89, U'Straße', U'Straße'},
                 {'name2', 2, false, 5.6, 7.8, D'00', -1234567.89, U'là', U'là'}], childrec);

// Set up the MySQL database

drop() := EMBED(mysql : user('rchapman'),database('test'))
  DROP TABLE IF EXISTS tbl1;
ENDEMBED;

create() := EMBED(mysql : user('rchapman'),database('test'))
  CREATE TABLE tbl1 ( name VARCHAR(20), value INT, boolval TINYINT, r8 DOUBLE, r4 FLOAT, d BLOB, ddd DECIMAL(10,2), u1 VARCHAR(10), u2 VARCHAR(10) );
ENDEMBED;

// Initialize the MySQL table, passing in the ECL dataset to provide the rows

initialize(dataset(childrec) values) := EMBED(mysql : user('rchapman'),database('test'))
  INSERT INTO tbl1 values (?, ?, ?, ?, ?, ?, ?, ?, ?);
ENDEMBED;

// Add an additional row containing NULL values, to test that they are handled properly when read in ECL

initializeNulls() := EMBED(mysql : user('rchapman'),database('test'))
  INSERT INTO tbl1 (name) values ('nulls');
ENDEMBED;

// Returning a dataset

dataset(childrec) testMySQLDS() := EMBED(mysql : user('rchapman'),database('test'))
  SELECT * from tbl1;
ENDEMBED;

// Returning a single row

childrec testMySQLRow() := EMBED(mysql : user('rchapman'),database('test'))
  SELECT * from tbl1 LIMIT 1;
ENDEMBED;

// Passing in parameters

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

// Returning scalars

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

// Passing in AND returning a dataset - this ends up acting a bit like a keyed join...

stringrec := RECORD
   string name
END;

stringrec extractName(childrec l) := TRANSFORM
  SELF := l;
END;

dataset(childrec) testMySQLDSParam(dataset(stringrec) inrecs) := EMBED(mysql : user('rchapman'),database('test'))
  SELECT * from tbl1 where name = ?;
ENDEMBED;

sequential (
  drop(),
  create(),
  initialize(init),
  initializeNulls(),
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
  OUTPUT(testMySQLUnicode()),
  OUTPUT(testMySQLDSParam(PROJECT(init, extractName(LEFT))))
);