IMPORT MYSQL;

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

staticData := DATASET([{'name1', 1, true, 1.2, 3.4, D'aa55aa55', 1234567.89, U'Straße', U'Straße'},
                       {'name2', 2, false, 5.6, 7.8, D'00', -1234567.89, U'là', U'là'}], childrec);

theData := DATASET('~test::test', childrec, THOR);

//-----------------------

drop() := EMBED(mysql : user('jenkins_test'), password('foo'), database('ba_test'), server('localhost'), port('3306'))
  DROP TABLE IF EXISTS tbl1;
ENDEMBED;

create() := EMBED(mysql : user('jenkins_test'), password('foo'), database('ba_test'), server('localhost'), port('3306'))
  CREATE TABLE tbl1 ( name VARCHAR(20), value INT, boolval TINYINT, r8 DOUBLE, r4 FLOAT, d BLOB, ddd DECIMAL(10,2), u1 VARCHAR(10), u2 VARCHAR(10) );
ENDEMBED;

initialize(dataset(childrec) values1) := EMBED(mysql : user('jenkins_test'), password('foo'), database('ba_test'), server('localhost'), port('3306'))
  INSERT INTO tbl1(name, value, boolval, r8, r4, d, ddd, u1, u2) values (?, ?, ?, ?, ?, ?, ?, ?, ?);
ENDEMBED;

//-----------------------

createDSAction := OUTPUT(staticData,, '~test::test', THOR, OVERWRITE);

testActions := SEQUENTIAL
    (
        drop(),
        create(),
        initialize(theData)
    );

//-----------------------

//createDSAction;
testActions;
