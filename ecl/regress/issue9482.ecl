FO := SERVICE
  {varstring512 mya, varstring512 myb} GetInfo(const varstring in_a, const varstring in_b):c,pure,entrypoint='GetInfo';
END;

{varstring512 mya, varstring512 myb} FOO(const varstring in_a, const varstring in_b):= BEGINC++
    struct FO_ { char mya[513]; char myb[513]; };
    FO_ * FOO = (FO_*)__result;
    strncpy(FOO->mya, in_a, 512);
    FOO->mya[512] = 0;
    strncpy(FOO->myb, in_b, 512);
    FOO->myb[512] = 0;
ENDC++;

FOO('a','b');
