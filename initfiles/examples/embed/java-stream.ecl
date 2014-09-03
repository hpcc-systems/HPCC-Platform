import java;

/*
 This example illustrates various calls to Java functions defined in the Java module JavaCat.
 The source of JavaCat can be found in the examples directory - it can be compiled to JavaCat.class
 using

   javac JavaCat

 and the resulting file JavaCat.class should be placed in /opt/HPCCSystems/classes (or somewhere else
 where it can be located via the standard Java CLASSPATH environment variable.
 */

// Passing and returning records and datasets
// When passing/returning a record, the corresponding Java function should take/return an object as a parameter whose fields
// can be mapped by name to the ECL record fields

nrec := record
  utf8 ufield;
end;

jret := RECORD
  boolean bfield;
  integer4 ifield;
  integer8 lfield;
  real8 dfield;
  real4 ffield;
  string1 cfield1;
  string1 cfield2;
  string sfield;
  nrec n;
  set of boolean bset;
  set of data dset;
  set of string sset;
  LINKCOUNTED DATASET(nrec) sub;
end;

jret jreturnrec(boolean b, integer i, real8 d) := IMPORT(java, 'JavaCat.returnrec:(ZID)LJavaCat;');
STRING jpassrec(jret r) := IMPORT(java, 'JavaCat.passrec:(LJavaCat;)Ljava/lang/String;');

ret := jreturnrec(false, 10, 2.345);

OUTPUT(ret);            // Calls a Java function that returns an ECL record
OUTPUT(jpassrec(ret));  // Passes an ECL record to a Java function

// When passing a dataset to a Java function, the Java function should take either an array or an iterator of objects,
// where the fields of the object in question are mapped by name to the fields in the ECL record.
// When passing an iterator, we use a modified form of the function signature in the IMPORT statement, using a < to indicate "Iterator of"
// followed by the name of the class of the objects that the iterator is to return. This is the one case where the output of javap -s cannot be used
// directly to provide the signature of the java function being called. We can also use the "extended" signature of the method that is output by
// javap -s -v, of the form 'JavaCat.passDataset:(Ljava/util/Iterator<LJavaCat;>;)I'
//
// To return a dataset, an iterator must be returned.

INTEGER passDataset(LINKCOUNTED DATASET(jret) d) :=
  IMPORT(java, 'JavaCat.passDataset:(<LJavaCat;)I'); // Calls int passDataset(Iterator<JavaCat> d)
// Note we could also use 'Ljava/util/Iterator<LJavaCat;>;)I' as the signature, but not 'Ljava/util/Iterator;)I' which is the signature output by javap -s

DATASET(jret) passDataset2(LINKCOUNTED DATASET(jret) d) :=
  IMPORT(java, 'JavaCat.passDataset2:([LJavaCat;)Ljava/util/Iterator;'); // Calls Iterator<JavaCat> passDataset2(JavaCat d[])

ds := DATASET(
  [
     {true, 1,2,3,4,'a', 'b', 'cd', u'ef', [true,false], [], ['Hello from ECL'], [{'1'},{'2'},{'3'},{'4'},{'5'}]}
    ,{true, 2,4,3,4,'a', 'b', 'cd', u'ef', [true,false], [], [], []}
    ,{true, 3,6,3,4,'a', 'b', 'cd', u'ef', [true,false], [], [], []}
    ,{true, 8,8,3,4,'a', 'b', 'cd', u'ef', [true,false], [d'AA55'], [], []}
  ], jret);

output(passDataset(ds));  // Using an iterator
output(passDataset2(ds)); // using an array, and illustrating the return of a dataset

// It is also possible to code a traonsform function in Java - both the parameter and the return type should be a
// Java object type that maps the fields of the ECL record by name.

transform(jret) testTransform(jret in, integer lim) := IMPORT(java, 'JavaCat.transform:(LJavaCat;I)LJavaCat;');

output(passDataset2(ds));
output(project(ds, testTransform(LEFT, COUNTER)));
