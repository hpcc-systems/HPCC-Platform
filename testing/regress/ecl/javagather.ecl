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

IMPORT Java;

J(STRING scope, STRING persistMode) := MODULE
    // Note the VOLATILE that stops the code generator from unexpectedly moving calls that it thinks are
    // independent of current context
    EXPORT unsigned Accumulator() := EMBED(Java: GLOBALSCOPE(scope), PERSIST(persistMode), VOLATILE)
    import java.io.*;
    class Accumulator implements Serializable
    {
        public Accumulator()
        {
        }
        public synchronized Accumulator accumulate(int i)
        {
            n = n+i;
            return this;
        }
        public Accumulator gather(byte [] other)
        {
            Accumulator o = deserialize(other);
            n = n + o.n;
            return this;
        }
        public int result()
        {
            return n;
        }
        public byte[] serialize()
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try
            {
                ObjectOutput out = new ObjectOutputStream(bos);   
                out.writeObject(this);
                out.flush();
            }
            catch (Exception e) 
            {
            }
            return bos.toByteArray();
        }

        private Accumulator deserialize(byte[] s)
        {
            ByteArrayInputStream bis = new ByteArrayInputStream(s);
            try 
            {
                ObjectInput in = new ObjectInputStream(bis);
                return (Accumulator) in.readObject(); 
            } 
            catch (Exception ex) 
            {
                return null;
            }
        }
        private int n = 0;
    }
    ENDEMBED;
    
    EXPORT unsigned accumulate(unsigned obj, integer i) := IMPORT(Java, 'accumulate');
    EXPORT data serialize(unsigned obj) := IMPORT(Java, 'serialize');
    EXPORT unsigned gather(unsigned obj1, data sobj2) := IMPORT(Java, 'gather');
    EXPORT integer result(unsigned obj1) := IMPORT(Java, 'result');
END;

j1 := J('Accum', 'channel');  // Used for local accumulation 
j2 := J('merge', 'channel');   // Used for merging the locally accumulated values

// Some data, representing distributed data we want to process locally on each channel, then roll up the results

r := record
  integer i;
end;

d := DISTRIBUTE(NOFOLD(DATASET([{1}, {2}, {3}], r)), i);

// We store a reference to our local Java object in each record

o := RECORD
  unsigned obj;
END;

o accum(r l, o r) := TRANSFORM
  SELF.obj := j1.accumulate(IF (r.obj=0,j1.Accumulator(),r.obj), l.i);
END;

accumulated := AGGREGATE(d, o, accum(LEFT, RIGHT), LOCAL);

// accumulated will have one record per thor channel, containing a reference to the java object that
// has been doing the accumulation, plus the value of the most recent call in SELF.v

sr := record
  DATA obj;    // serialized object
end;

tomerge := DISTRIBUTE(PROJECT(accumulated(obj != 0), TRANSFORM(sr, SELF.obj := j1.serialize(LEFT.obj))), 0);

// tomerge has serialized all the java objects for each channel's results, and brought them together onto
// node 0 to combine the results. Note the NOFOLD(accumulated) - without this, the compiler may decide that
// the field 'v' is not used, and optimize it away (along with all the calls to accumulate).

o domerge(sr l, o r) := TRANSFORM
  SELF.obj := j2.gather(IF (r.obj=0,j2.Accumulator(), r.obj),l.obj);
END;

merged := AGGREGATE(tomerge, o, domerge(LEFT, RIGHT), LOCAL);

// similar to above, but now passing all the serialized objects in to the gather function


choosen(PROJECT(merged, TRANSFORM({integer result}, SELF.result := j2.result(LEFT.obj))), 1)[1].result;

// Extract the result from the java object in which we did the gathering

// Note: j2.result(merged[1].obj); would work on hthor but in thor is liable to execute on a node where obj is not valid