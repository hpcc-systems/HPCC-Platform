/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems.

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

class javaembed_library
{
  public static String cat(String input[])
  {
    StringBuffer ret = new StringBuffer();
    for (String item:input)
    {
      if (ret.length() > 0)
        ret.append(" ");
      ret.append(item);
    }
    return ret.toString();
  }

  public static int queryInit()
  {
    return initialized;
  }

  public static int queryConstructed()
  {
    return constructed;
  }

  static int initialized = 0;
  static int constructed = 0;

  static {
    System.out.println("This is a static block");
    initialized++;
  }

  public javaembed_library(){
     System.out.println("This is a constructor");
     constructed++;
  }

}
