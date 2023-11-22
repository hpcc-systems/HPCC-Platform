#include "hpcc_scalar_test.h"

#include <string>

void dbglog(const std::string str)
{
    hpcc_scalar_test_string_t msg;
    hpcc_scalar_test_string_set(&msg, str.c_str());
    hpcc_scalar_test_dbglog(&msg);
}

bool hpcc_scalar_test_bool_test(bool a, bool b)
{
    return a && b;
}
float hpcc_scalar_test_float32_test(float a, float b)
{
    return a + b;
}
double hpcc_scalar_test_float64_test(double a, double b)
{
    return a + b;
}
uint8_t hpcc_scalar_test_u8_test(uint8_t a, uint8_t b)
{
    return a + b;
}
uint16_t hpcc_scalar_test_u16_test(uint16_t a, uint16_t b)
{
    return a + b;
}
uint32_t hpcc_scalar_test_u32_test(uint32_t a, uint32_t b)
{
    return a + b;
}
uint64_t hpcc_scalar_test_u64_test(uint64_t a, uint64_t b)
{
    return a + b;
}
int8_t hpcc_scalar_test_s8_test(int8_t a, int8_t b)
{
    return a + b;
}
int16_t hpcc_scalar_test_s16_test(int16_t a, int16_t b)
{
    return a + b;
}
int32_t hpcc_scalar_test_s32_test(int32_t a, int32_t b)
{
    return a + b;
}
int64_t hpcc_scalar_test_s64_test(int64_t a, int64_t b)
{
    return a + b;
}
uint32_t hpcc_scalar_test_char_test(uint32_t a, uint32_t b)
{
    return a + b;
}
static uint32_t tally = 0;
void hpcc_scalar_test_utf8_string_test(hpcc_scalar_test_string_t *a, hpcc_scalar_test_string_t *b, hpcc_scalar_test_string_t *ret)
{
    std::string s1(a->ptr, a->len);
    hpcc_scalar_test_string_free(a);
    std::string s2(b->ptr, b->len);
    hpcc_scalar_test_string_free(b);
    std::string r = s1 + s2;
    dbglog(std::to_string(++tally) + ":  " + r);
    hpcc_scalar_test_string_dup(ret, r.c_str());
}
