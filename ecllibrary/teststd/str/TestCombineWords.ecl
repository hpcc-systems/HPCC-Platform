/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestCombineWords := MODULE
  EXPORT TestRuntime := MODULE
    EXPORT Test01 := ASSERT(Str.CombineWords([],',') = '');
    EXPORT Test02 := ASSERT(Str.CombineWords(['x'],',') = 'x');
    EXPORT Test03 := ASSERT(Str.CombineWords(['x','y'],',') = 'x,y');
    EXPORT Test04 := ASSERT(Str.CombineWords(['',''],',') = ',');
    EXPORT Test05 := ASSERT(Str.CombineWords(['',''],'') = '');
    EXPORT Test06 := ASSERT(Str.CombineWords(['abc','def','ghi'],'') = 'abcdefghi');
  END;
END;
