/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems.

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

import java;

/*
 Similar to java-stream example, but using inline Java 
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

jret returnrec(boolean b, integer i, real8 d) := EMBED(java)
import java.util.*;
public class Test1
{
  public static class NestedClass
  {
    String ufield;
    public NestedClass(String s)
    {
      ufield = s;
    }
    public NestedClass()
    {
    }
  }

  boolean bfield;
  int ifield;
  long lfield;
  double dfield;
  float ffield;
  String sfield;
  char cfield1;
  String cfield2;
  NestedClass n;
  boolean bset[];
  byte [] dset[];
  String sset[];
  NestedClass sub[];

  public Test1(boolean b, int i, double d)
  {
    bfield = b;
    ifield = i;
    lfield = i * 100000000;
    dfield = d;
    ffield = (float) d;
    sfield = "Yoohoo";
    cfield1 = 'X';
    cfield2 = "Z";
    n = new NestedClass("nest");
    bset = new boolean [5];
    bset[3] = b;
    dset = new byte[1][];
    dset[0] = new byte[1];
    dset[0][0] = 14;
    sset = new String[1];
    sset[0] = "Hello";
    sub = new NestedClass[1];
    sub[0] = new NestedClass("subnest");
  }

  public Test1()
  {
    n = new NestedClass("nest2");
  }

  public static Test1 returnrec(boolean b, int i, double d)
  {
    return new Test1(b,i,d);
  }
}
ENDEMBED;


STRING passrec(jret r) := EMBED(java)
import java.util.*;
public class Test2
{
  public static class NestedClass
  {
    String ufield;
    public NestedClass(String s)
    {
      ufield = s;
    }
    public NestedClass()
    {
    }
  }

  boolean bfield;
  int ifield;
  long lfield;
  double dfield;
  float ffield;
  String sfield;
  char cfield1;
  String cfield2;
  NestedClass n;
  boolean bset[];
  byte [] dset[];
  String sset[];
  NestedClass sub[];

  public Test2()
  {
    n = new NestedClass("nest2");
  }

  public static String passrec(Test2 j)
  {
    return j.n.ufield;
  }
}
ENDEMBED;

ret := returnrec(false, 10, 2.345);

OUTPUT(ret);            // Calls a Java function that returns an ECL record
OUTPUT(passrec(ret));  // Passes an ECL record to a Java function

// When passing a dataset to a Java function, the Java function should take either an array or an iterator of objects,
// where the fields of the object in question are mapped by name to the fields in the ECL record.
//
// To return a dataset, an iterator must be returned.

INTEGER passDataset(LINKCOUNTED DATASET(jret) d) := EMBED(Java)
import java.util.*;
public class Test3
{
  public static class NestedClass
  {
    String ufield;
    public NestedClass(String s)
    {
      ufield = s;
    }
    public NestedClass()
    {
    }
  }

  boolean bfield;
  int ifield;
  long lfield;
  double dfield;
  float ffield;
  String sfield;
  char cfield1;
  String cfield2;
  NestedClass n;
  boolean bset[];
  byte [] dset[];
  String sset[];
  NestedClass sub[];

  public Test3()
  {
    n = new NestedClass("nest2");
  }

  public static int passDataset(Iterator<Test3> d)
  {
    int sum = 0;
    while (d.hasNext())
    {
      Test3 r = d.next();
      sum += r.lfield;
    }
    return sum;
  }

}
ENDEMBED;

DATASET(jret) passDataset2(LINKCOUNTED DATASET(jret) d) := EMBED(Java)
import java.util.*;
public class Test4
{
  public static class NestedClass
  {
    String ufield;
    public NestedClass(String s)
    {
      ufield = s;
    }
    public NestedClass()
    {
    }
  }

  boolean bfield;
  int ifield;
  long lfield;
  double dfield;
  float ffield;
  String sfield;
  char cfield1;
  String cfield2;
  NestedClass n;
  boolean bset[];
  byte [] dset[];
  String sset[];
  NestedClass sub[];

  public Test4()
  {
    n = new NestedClass("nest2");
  }

  public static Iterator<Test4> passDataset2(Test4 d[])
  {
    return Arrays.asList(d).iterator();
  }
}

ENDEMBED;

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

transform(jret) testTransform(jret in, integer lim) := EMBED(java)
import java.util.*;
public class Test5
{
  public static class NestedClass
  {
    String ufield;
    public NestedClass(String s)
    {
      ufield = s;
    }
    public NestedClass()
    {
    }
  }

  boolean bfield;
  int ifield;
  long lfield;
  double dfield;
  float ffield;
  String sfield;
  char cfield1;
  String cfield2;
  NestedClass n;
  boolean bset[];
  byte [] dset[];
  String sset[];
  NestedClass sub[];

  public Test5(boolean b, int i, double d)
  {
    bfield = b;
    ifield = i;
    lfield = i * 100000000;
    dfield = d;
    ffield = (float) d;
    sfield = "Yoohoo";
    cfield1 = 'X';
    cfield2 = "Z";
    n = new NestedClass("nest");
    bset = new boolean [5];
    bset[3] = b;
    dset = new byte[1][];
    dset[0] = new byte[1];
    dset[0][0] = 14;
    sset = new String[1];
    sset[0] = "Hello";
    sub = new NestedClass[1];
    sub[0] = new NestedClass("subnest");
  }

  public Test5()
  {
    n = new NestedClass("nest2");
  }

  public static Test5 testTransform(Test5 in, int lim)
  {
    return new Test5(in.bfield, lim, in.dfield);
  }

}
ENDEMBED;

output(project(ds, testTransform(LEFT, COUNTER)));
