import java.util.*;
public class JavaCat
{
  public static int add1(int a)
  {
    return a + 1;
  }
  public static String add2(String a)
  {
    return a + '1';
  }
  public static char addChar(char a)
  {
    return ++a;
  }
  public static int testThrow(int a) throws Exception
  {
    throw new Exception("Exception from Java");
  }

  public static byte[] testData(byte [] indata)
  {
    indata[0]++;
    return indata;
  }

  public static int add(int a, int b)
  {
    return a + b;
  }
  public static long addL(int a, int b)
  {
    return a + b;
  }
  public static Integer addI(int a, int b)
  {
    return a + b;
  }
  public static float fadd(float a, float b)
  {
    System.out.print("fadd(");
    System.out.print(a);
    System.out.print(",");
    System.out.print(b);
    System.out.println(")");
    return a + b;
  }
  public static double dadd(double a, double b)
  {
    System.out.print("fadd(");
    System.out.print(a);
    System.out.print(",");
    System.out.print(b);
    System.out.println(")");
    return a + b;
  }
  public static Double daddD(double a, double b)
  {
    System.out.print("fadd(");
    System.out.print(a);
    System.out.print(",");
    System.out.print(b);
    System.out.println(")");
    return a + b;
  }
  public static String cat(String a, String b)
  {
    return a + b;
  }

  public static int testArrays(boolean[] b, short[] s, int[] i, double[] d)
  {
    return b.length + s.length + i.length + d.length;
  }

  public static String[] testStringArray(String[] in)
  {
    String t = in[0];
    in[0] = in[1];
    in[1] = t;
    return in;
  }

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

  public JavaCat(boolean b, int i, double d)
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

  public JavaCat()
  {
    n = new NestedClass("nest2");
  }

  public static JavaCat returnrec(boolean b, int i, double d)
  {
    return new JavaCat(b,i,d);
  }

  public static String passrec(JavaCat j)
  {
    return j.n.ufield;
  }

  public static JavaCat transform(JavaCat in, int lim)
  {
    return new JavaCat(in.bfield, lim, in.dfield);
  }

  public static int passDataset(Iterator<JavaCat> d)
  {
    int sum = 0;
    while (d.hasNext())
    {
      JavaCat r = d.next();
      System.out.print(r.lfield);
      System.out.println("");
      sum += r.lfield;
    }
    return sum;
  }

  public static Iterator<JavaCat> passDataset2(JavaCat d[])
  {
    return Arrays.asList(d).iterator();
  }
}
