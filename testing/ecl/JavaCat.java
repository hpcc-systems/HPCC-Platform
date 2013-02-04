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
}
