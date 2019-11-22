IMPORT Java;

J(STRING x) := MODULE
    EXPORT unsigned Accumulator(DATA s = d'') := EMBED(Java: PERSIST('workunit'), GLOBALSCOPE(x), VOLATILE)
    import java.io.*;
    class Accumulator implements Serializable
    {
        public Accumulator(byte[] s)
        {
            System.out.print("construct ");
            if (s.length!=0)
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(s);
                try
                {
                    ObjectInput in = new ObjectInputStream(bis);
                    System.out.print("deserialize ");
                    Accumulator other = (Accumulator) in.readObject();
                    n = other.n;
                }
                catch (Exception ex)
                {
                }
            }
            System.out.println(n);
        }
        public synchronized int accumulate(int i)
        {
            System.out.print("accumulate ");
            System.out.print(n);
            System.out.print(" + ");
            System.out.println(i);
            n = n+i;
            return n;
        }
        public int gather(byte [] other)
        {
            Accumulator o = new Accumulator(2, other);
            System.out.print("gather ");
            System.out.print(n);
            System.out.print(" + ");
            System.out.println(o.n);
            n = n + o.n;
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
        private int n = 0;
    }
    ENDEMBED;

    EXPORT integer accumulate(unsigned obj, integer i) := IMPORT(Java, 'accumulate');
    EXPORT data serialize(unsigned obj) := IMPORT(Java, 'serialize');
    EXPORT integer gather(unsigned obj1, data sobj2) := IMPORT(Java, 'gather');
END;

j1 := J('Accum');
j2 := J('merge');

r := record
  integer i;
end;

o := RECORD
  unsigned obj;
  integer v;
END;

o accum(r l, o r) := TRANSFORM
  SELF.obj := IF (r.obj=0,j1.Accumulator(d''),r.obj);
  SELF.v := j1.accumulate(SELF.obj, l.i);
END;

d1 := DISTRIBUTE(NOFOLD(DATASET([{1}, {2}, {3}], r)), i);

accumulated := AGGREGATE(d1, o, accum(LEFT, RIGHT), LOCAL);

sr := record
  DATA obj;    // serialized object
end;

tomerge := DISTRIBUTE(PROJECT(NOFOLD(accumulated), TRANSFORM(sr, SELF.obj := j1.serialize(LEFT.obj))), 0);

o domerge(sr l, o r) := TRANSFORM
  SELF.obj := IF (r.obj=0,j2.Accumulator(d''), r.obj);
  SELF.v := j2.gather(SELF.obj, l.obj);
END;

merged := AGGREGATE(tomerge, o, domerge(LEFT, RIGHT), LOCAL);
merged[1].v;
