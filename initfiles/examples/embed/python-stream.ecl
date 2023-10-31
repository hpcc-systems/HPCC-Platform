IMPORT Python3 as Python;

/*
 This example illustrates and tests the use of embedded Python.
 */

// This example illustrates returning datasets from Python

// These are the record structures we will be returning - note the child records, datasets and dictionaries in it

childrec := RECORD
   STRING name => UNSIGNED value;
END;

eclRecord := RECORD
    STRING name1;
    STRING10 name2;
    LINKCOUNTED DATASET(childrec) childnames;
    LINKCOUNTED DICTIONARY(childrec) childdict{LINKCOUNTED};
    childrec r;
    UNSIGNED1 val1;
    INTEGER1   val2;
    UTF8 u1;
    UNICODE u2;
    UNICODE8 u3;
    BIG_ENDIAN unsigned4 val3;
    DATA d;
    BOOLEAN b;
    SET OF STRING ss1;
END;

namerec := RECORD
   STRING name;
END;

namerec2 := RECORD
   STRING name;
   STRING name2;
END;

// To return a dataset, we can return a list of tuples, each one correponding to a field in the resulting ECL record
// In this example, the fields are mapped by position
// Just to spice things up we proved a couple of parameters too

DATASET(eclRecord) streamedNames(DATA d, utf8 u) := EMBED(Python)
  return [  \
     ("Gavin", "Halliday", [("a", 1),("b", 2),("c", 3)], [("aa", 11)], ("aaa", 111), 250, -1,  U'là',  U'là',  U'là', 0x01000000, d, False, {"1","2"}), \
     ("John", "Smith", [], [], ("c", 3), 250, -1,  U'là',  U'là',  u, 0x02000000, d, True, []) \
     ]
ENDEMBED;

OUTPUT(streamedNames(d'AA', u'là'));

// We can also return a dataset by using a Python generator, which will be lazy-evaluated as the records are required by ECL code...

DATASET(childrec) testGenerator(unsigned lim) := EMBED(Python)
  num = 0
  while num < lim:
    yield ("Generated", num)
    num += 1
ENDEMBED;

OUTPUT (testGenerator(10));

// If the returned tuples are namedtuples, we map fields by name rather than by position

// Test use of Python named tuple...

DATASET(childrec) testNamedTuples() := EMBED(Python)
  import collections
  ChildRec = collections.namedtuple("childrec", "value, name") # Note - order is reverse of childrec - but works as we get fields by name
  c1 = ChildRec(1, "name1")
  c2 = ChildRec(name="name2", value=2)
  return [ c1, c2 ]
ENDEMBED;

OUTPUT(testNamedTuples());

// To return a record, just return a tuple (or namedtuple)

childrec testRecord(integer value, string s) := EMBED(Python)
  return (s, value)
ENDEMBED;

OUTPUT(testRecord(1,'Hello').value);
OUTPUT(testRecord(1,'Hello').name);

// If the record has a single field, you don't need to put the field into a tuple...

DATASET(namerec) testMissingTuple1(unsigned lim) := EMBED(Python)
  return [ '1', '2', '3' ]
ENDEMBED;
OUTPUT (testMissingTuple1(10));

// ... but you can if you want

DATASET(namerec) testMissingTuple2(unsigned lim) := EMBED(Python)
  return [ ('1'), ('2'), ('3') ]
ENDEMBED;
OUTPUT (testMissingTuple2(10));

// You can define a transform in Python, using a function that returns a record (i.e. a Python tuple)
// Note that the tuple we pass to Python is a namedtuple

TRANSFORM(childrec) testTransform(namerec inrec, unsigned c) := EMBED(Python)
  return (inrec.name, c)
ENDEMBED;

d := DATASET([{'Richard'},{'Gavin'}], namerec);

OUTPUT(PROJECT(d, testTransform(LEFT, COUNTER)));

// Most transforms take a record as the input, but it's not a requirement

TRANSFORM(childrec) testTransformNoRow(unsigned lim) := EMBED(Python)
  return ("Hello", lim)
ENDEMBED;

OUTPUT(ROW(testTransformNoRow(10)));

// When passing datasets to Python, we get an iterator of named tuples
// They are actually implemented as generators, meaning they are lazy-evaluated

names := DATASET([{'Richard'}, {'James'}, {'Andrew'}], namerec);

STRING datasetAsIterator(DATASET(namerec) input) := EMBED(Python)
  s = ''
  for n in input:
    s = s + ' ' + n.name
  return s;
ENDEMBED;
OUTPUT(datasetAsIterator(names));
