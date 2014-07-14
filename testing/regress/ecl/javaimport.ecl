import java;
string jcat(string a, string b) := IMPORT(java, 'JavaCat.cat:(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;' : classpath('/opt/HPCCSystems/classes'));

integer jadd(integer a, integer b) := IMPORT(java, 'JavaCat.add:(II)I');
integer jaddl(integer a, integer b) := IMPORT(java, 'JavaCat.addL:(II)J');
integer jaddi(integer a, integer b) := IMPORT(java, 'JavaCat.addI:(II)Ljava/lang/Integer;');

real jfadd(real4 a, real4 b) := IMPORT(java, 'JavaCat.fadd:(FF)F');
real jdadd(real a, real b) := IMPORT(java, 'JavaCat.dadd:(DD)D');
real jdaddD(real a, real b) := IMPORT(java, 'JavaCat.daddD:(DD)Ljava/lang/Double;');

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

/*
jret jreturnrec(boolean b, integer i, real8 d) := IMPORT(java, 'JavaCat.returnrec:(ZID)LJavaCat;');
STRING jpassrec(jret r) := IMPORT(java, 'JavaCat.passrec:(LJavaCat;)Ljava/lang/String;');

jcat('Hello ', 'world!');
jadd(1,2);
jaddl(3,4);
jaddi(5,6);

jfadd(1,2);
jdadd(3,4);
jdaddD(5,6);
ret := jreturnrec(false, 10, 2.345);
ret;
jpassrec(ret);
*/

DATASET(jret) passDataset2(LINKCOUNTED DATASET(jret) d) := IMPORT(java, 'JavaCat.passDataset2:([LJavaCat;)Ljava/util/Iterator;');

ds := DATASET(
  [
     {true, 1,2,3,4,'a', 'b', 'cd', u'ef', [true,false], [], ['Hello from ECL'], [{'1'},{'2'},{'3'},{'4'},{'5'}]}
    ,{true, 2,4,3,4,'a', 'b', 'cd', u'ef', [true,false], [], [], []}
    ,{true, 3,6,3,4,'a', 'b', 'cd', u'ef', [true,false], [], [], []}
    ,{true, 8,8,3,4,'a', 'b', 'cd', u'ef', [true,false], [d'AA55'], [], []}
  ], jret);

transform(jret) testTransform(jret in, integer lim) := IMPORT(java, 'JavaCat.transform:(LJavaCat;I)LJavaCat;');

output(passDataset2(ds));
output(project(ds, testTransform(LEFT, COUNTER)));
