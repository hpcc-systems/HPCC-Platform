/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

IMPORT Java;

//class=embedded
//class=3rdparty

//version forceNonThread=false
//version forceNonThread=true


import ^ as root;
forceNonThread := #IFDEFINED(root.forceNonThread, false);

// Check that implicitly-created objects have appropriate lifetime

implicit(STRING p, STRING s) := MODULE
  STRING st := ''
#if (forceNonThread)
   : STORED('st')
#end
  ;

  EXPORT INTEGER accumulate(INTEGER b) := EMBED(Java : PERSIST(p), GLOBALSCOPE(s+st))
    class x
    {
      public x()
      {
        synchronized (x.class)
        { 
          idx = nextIdx;
          nextIdx = nextIdx+1;
        }
//        System.out.println("created  " + idx + x.class.getName());
      }
      public void finalize()
      {
//        System.out.println("finalize " + n + " " + idx);
      }
      public synchronized int accumulate(int b)
      {
        n = n + b;
        return n;
      }
      int n = 0;
      int idx = 0;
      static int nextIdx = 0;
    }
  ENDEMBED;

  SHARED r := RECORD
    integer a; 
  END;


  // The parallel test runs all on separate threads (except the last two calls) to ensure that separate threads
  // are independent when using PERSIST('thread') or PERSIST('none')

  EXPORT ptest := PARALLEL (
    output(p + ': parallel');
    output(project(nofold(dataset([{1}], r)), transform(r, self.a := accumulate(LEFT.a))));
    output(project(nofold(dataset([{2}], r)), transform(r, self.a := accumulate(LEFT.a))));
    output(project(nofold(dataset([{3}], r)), transform(r, self.a := accumulate(LEFT.a))));
    output(project(nofold(dataset([{10}], r)), transform(r, self.a := accumulate(LEFT.a)+accumulate(LEFT.a*2))));
  );

  // The sequential test runs sequentially for ones that are supposed to interact across threads (otherwise results are indeterminate)

  EXPORT stest := ORDERED (
    output(p + ': sequential');
    output(project(nofold(dataset([{1}], r)), transform(r, self.a := accumulate(LEFT.a))));
    output(project(nofold(dataset([{2}], r)), transform(r, self.a := accumulate(LEFT.a))));
    output(project(nofold(dataset([{3}], r)), transform(r, self.a := accumulate(LEFT.a))));
    output(project(nofold(dataset([{10}], r)), transform(r, self.a := accumulate(LEFT.a)+accumulate(LEFT.a*2))));
  );

END;

gc() := EMBED(Java)
  public static void gc()
  {
    System.gc();
  }
ENDEMBED;

ORDERED (
  implicit('','').ptest;
  implicit('','').stest;
  implicit('thread','').ptest;
  implicit('channel','').stest;
  implicit('query','').stest;
  implicit('workunit','').stest;
//  implicit('global','').stest;
//  gc();
);

// Check that explicitly-created objects have appropriate lifetime (how?) but are not shared

