rtl := SERVICE
 unsigned4 sleep(unsigned4 _delay) : eclrtl,action,library='eclrtl',entrypoint='rtlSleep';
END;

s := 0 : STORED('s');

MyRec := RECORD
    STRING5 Value1;
END;

MyRec t1(unsigned c) := transform
  SELF.value1 := 'HELL'+rtl.sleep(s);
END;

ds := NOFOLD(DATASET(1, t1(COUNTER)));
a(STRING f) := allnodes(ds(Value1=f));
b(STRING f) := a(f+'1')+a(f+'2')+a(f+'3')+a(f+'4')+a(f+'5')+a(f+'6')+a(f+'7')+a(f+'8')+a(f+'9')+a(f+'10');
c := b('1')+b('2')+b('3')+b('4')+b('5')+b('6')+b('7')+b('8')+b('9')+b('10');
c;
