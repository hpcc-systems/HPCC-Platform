import lib_exampleplugin;

s := service : fold,library('eclrtl')
  integer4 rtlCompareVStrVStr(const varstring a, const varstring b) : pure,CPP;
END;

ASSERT(s.rtlCompareVStrVStr('1','1')=0, CONST);
ASSERT(s.rtlCompareVStrVStr('1','0')>0, CONST);
ASSERT(s.rtlCompareVStrVStr('1','2')<0, CONST);
ASSERT(exampleplugin.test1(1,2,3,4,5,6,7,8,9,10)= '1 2 3 4 5 6 7 8 9 10', CONST);
ASSERT(exampleplugin.test2(1,2,3,4,5,6,7,8)= '1.000 2.000 3.000 4.000 5.000 6.000 7.000 8.000', CONST);
ASSERT(exampleplugin.test3(1,2,3,4,5,6,7,8,9,10,101,102,103,104,105,106,107,108) = '1 2 3 4 5 6 7 8 9 10 101.000 102.000 103.000 104.000 105.000 106.000 107.000 108.000', CONST);
ASSERT(exampleplugin.test4('123', '456', '789', '101') = '123,456       ,789,101', CONST);
OUTPUT('ok');
