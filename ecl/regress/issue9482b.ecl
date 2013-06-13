
{varstring mya, varstring myb} FOO(const varstring in_a, const varstring in_b):= BEGINC++
    size_t lena = strlen(in_a)+1;
    size_t lenb = strlen(in_a)+1;
    byte * target = (byte*)__result;
    memcpy(target, in_a, lena);
    memcpy(target+lena, in_b, lenb);
ENDC++;

FOO('abcbdeb','bdedeef');
