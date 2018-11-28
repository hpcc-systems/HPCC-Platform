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

import java;
string jcat(string a, string b) := EMBED(java)
  public static String cat(String a, String b)
  {
    return a + b;
  }
ENDEMBED;

integer jadd(integer a, integer b) := EMBED(java)
  public static int add(int a, int b)
  {
    return a + b;
  }
ENDEMBED;
integer jaddl(integer a, integer b) := EMBED(java)
  public static long addL(int a, int b)
  {
    return a + b;
  }
ENDEMBED;
integer jaddi(integer a, integer b) := EMBED(java)
  public static Integer addI(int a, int b)
  {
    return a + b;
  }
ENDEMBED;

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

DATASET(jret) passDataset2(LINKCOUNTED DATASET(jret) d) := EMBED(java)
import java.util.*;
public class myClass {
  public static class MyRecord {

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
    };

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

    public MyRecord(boolean b, int i, double d)
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
    public MyRecord()  // This will be called to construct objects being passed in from ECL
    {
      n = new NestedClass("nest2");
    }
  };
  public static Iterator<MyRecord> passDataset2(MyRecord d[])
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

transform(jret) testTransform(jret in, integer lim) := EMBED(java)
public class myClass {
  public static class MyRecord {

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
    };

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

    public MyRecord(boolean b, int i, double d)
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
    public MyRecord()  // This will be called to construct objects being passed in from ECL
    {
      n = new NestedClass("nest2");
    }
  };
  public static MyRecord transform(MyRecord in, int lim)
  {
    return new MyRecord(in.bfield, lim, in.dfield);
  }
}
ENDEMBED;

jcat('Hello, ', 'Java');
jadd(1,2);
jaddl(3,4);
jaddi(5,6);

output(passDataset2(ds));
output(project(ds, testTransform(LEFT, COUNTER)));
