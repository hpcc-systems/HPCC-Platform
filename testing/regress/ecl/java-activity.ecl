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

IMPORT Java, Std;
 
kString := RECORD
    INTEGER4 id;
    STRING text;
END;
 
// First test that the activity context parameter can be used

DATASET(kString) withParam(STREAMED DATASET(kString) inp, integer4 slaveNo, integer4 numSlaves) := EMBED(Java: activity)
import java.util.*;
import com.HPCCSystems.ActivityContext;
public class myClass {

  public static class kString {
    String text;
    int id;
    public kString() {}
    public kString(String _text, int _id)
    {
      text = _text;
      id = _id;
    }
  };

  public static Iterator<kString> withParam(ActivityContext ctx, Iterator<kString> in, int slaveNo, int numSlaves)
  {
    List<kString> list = new ArrayList<>();
    while (in.hasNext())
    {
        list.add(in.next());
    }
    if (slaveNo==0)
    {
        list.add(new kString("withParam", slaveNo));
        list.add(new kString("ctx.numSlaves-numslaves", ctx.numSlaves()- numSlaves));  // SHould be 0
        list.add(new kString("ctx.numStrands", ctx.numStrands()));
        list.add(new kString("ctx.querySlave", ctx.querySlave()));
        list.add(new kString("ctx.queryStrand", ctx.queryStrand()));
        list.add(new kString("ctx.isLocal", ctx.isLocal() ? 1 : 0));
    }
    else if (ctx.querySlave() != slaveNo)
        list.add(new kString("Unexpected ctx.querySlave value", ctx.querySlave()));
    
    return list.iterator();
  }
}
ENDEMBED;

// Also test that you don't need to include it if you don't want to
 
DATASET(kString) withoutParam(STREAMED DATASET(kString) inp, integer4 slaveNo, integer4 numSlaves) := EMBED(Java: activity)
import java.util.*;
public class myClass {

  public static class kString {
    String text;
    int id;
    public kString() {}
    public kString(String _text, int _id)
    {
      text = _text;
      id = _id;
    }
  };

  public static Iterator<kString> withoutParam(kString in[], int slaveNo, int numSlaves)
  {
    List<kString> list = new ArrayList<>();
    for (kString inrec : in)
    {
        list.add(inrec);
    }
    if (slaveNo==0)
        list.add(new kString("withoutParam", slaveNo));
    return list.iterator();
  }
}
ENDEMBED;

testInput := DISTRIBUTE(DATASET([{88, 'Input'}], kString), 0);    // Data on node 1 only

o0 := output(testInput);
o1 := output(withParam(testInput, Std.System.thorlib.node(), Std.System.thorlib.nodes()));
o2 := output(withoutParam(testInput, Std.System.thorlib.node(), Std.System.thorlib.nodes()));
o3 := output(withParam(testInput, Std.System.thorlib.node(), Std.System.thorlib.nodes()));
 
SEQUENTIAL(o0, o1, o2, o3);
