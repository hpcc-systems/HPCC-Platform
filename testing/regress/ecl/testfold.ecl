s := service : fold,library('eclrtl')
  integer4 rtlCompareVStrVStr(const varstring a, const varstring b) : pure,CPP;
END;

ASSERT(s.rtlCompareVStrVStr('1','1')=0, CONST);
ASSERT(s.rtlCompareVStrVStr('1','0')>0, CONST);
ASSERT(s.rtlCompareVStrVStr('1','2')<0, CONST);
OUTPUT('ok');
