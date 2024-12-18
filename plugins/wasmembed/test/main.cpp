#include "hpcc_scalar_test.h"

#include <string>
#include <vector>

void dbglog(const std::string str)
{
    hpcc_scalar_test_string_t msg;
    hpcc_scalar_test_string_set(&msg, str.c_str());
    hpcc_scalar_test_dbglog(&msg);
}

bool hpcc_scalar_test_bool_and_test(bool a, bool b)
{
    return a && b;
}
bool hpcc_scalar_test_bool_or_test(bool a, bool b)
{
    return a || b;
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

// Helper Functions
// void hpcc_scalar_test_list_u32_free(hpcc_scalar_test_list_u32_t *ptr);
// void hpcc_scalar_test_string_set(hpcc_scalar_test_string_t *ret, const char*s);
// void hpcc_scalar_test_string_dup(hpcc_scalar_test_string_t *ret, const char*s);
// void hpcc_scalar_test_string_free(hpcc_scalar_test_string_t *ret);

void hpcc_scalar_test_list_test_bool(hpcc_scalar_test_list_bool_t *ret)
{
    ret->len = 8;
    ret->ptr = (bool *)malloc(ret->len * sizeof(bool));
    for (size_t i = 0; i < ret->len; ++i)
    {
        ret->ptr[i] = i % 2 == 0;
    }
}

void hpcc_scalar_test_list_bool_test_bool(hpcc_scalar_test_list_bool_t *a, hpcc_scalar_test_list_bool_t *ret)
{
    std::vector<bool> v1(a->ptr, a->ptr + a->len);
    hpcc_scalar_test_list_bool_free(a);
    ret->len = v1.size();
    ret->ptr = (bool *)malloc(ret->len * sizeof(bool));
    for (size_t i = 0; i < ret->len; ++i)
    {
        ret->ptr[ret->len - i - 1] = v1[i];
    }
}

void hpcc_scalar_test_list_test_u32(hpcc_scalar_test_list_u32_t *ret)
{
    ret->len = 4;
    ret->ptr = (uint32_t *)malloc(ret->len * sizeof(uint32_t));
    for (size_t i = 0; i < ret->len; ++i)
    {
        ret->ptr[i] = i;
    }
}

void hpcc_scalar_test_list_u32_test_u32(hpcc_scalar_test_list_u32_t *a, hpcc_scalar_test_list_u32_t *ret)
{
    std::vector<uint32_t> v1(a->ptr, a->ptr + a->len);
    hpcc_scalar_test_list_u32_free(a);
    ret->len = v1.size();
    ret->ptr = (uint32_t *)malloc(ret->len * sizeof(uint32_t));
    for (size_t i = 0; i < ret->len; ++i)
    {
        ret->ptr[ret->len - i - 1] = v1[i];
    }
}

void hpcc_scalar_test_list_test_float32(hpcc_scalar_test_list_float32_t *ret)
{
    ret->len = 4;
    ret->ptr = (float *)malloc(ret->len * sizeof(float));
    for (size_t i = 0; i < ret->len; ++i)
    {
        ret->ptr[i] = i + 0.33;
    }
}

void hpcc_scalar_test_list_float32_test_float32(hpcc_scalar_test_list_float32_t *a, hpcc_scalar_test_list_float32_t *ret)
{
    std::vector<float> v1(a->ptr, a->ptr + a->len);
    hpcc_scalar_test_list_float32_free(a);
    ret->len = v1.size();
    ret->ptr = (float *)malloc(ret->len * sizeof(float));
    for (size_t i = 0; i < ret->len; ++i)
    {
        ret->ptr[ret->len - i - 1] = v1[i];
    }
}

void hpcc_scalar_test_list_test_string(hpcc_scalar_test_list_string_t *ret)
{
    ret->len = 4;
    ret->ptr = (hpcc_scalar_test_string_t *)malloc(ret->len * sizeof(hpcc_scalar_test_string_t));
    for (size_t i = 0; i < ret->len; ++i)
    {
        std::string str = "test-";
        str += std::to_string(i);
        hpcc_scalar_test_string_dup(&ret->ptr[i], str.c_str());
    }
}

void hpcc_scalar_test_list_string_test_string(hpcc_scalar_test_list_string_t *a, hpcc_scalar_test_list_string_t *ret)
{
    std::vector<hpcc_scalar_test_string_t> v1(a->ptr, a->ptr + a->len);
    ret->len = v1.size();
    ret->ptr = (hpcc_scalar_test_string_t *)malloc(ret->len * sizeof(hpcc_scalar_test_string_t));
    for (size_t i = 0; i < ret->len; ++i)
    {
        hpcc_scalar_test_string_dup(&ret->ptr[ret->len - i - 1], std::string(v1[i].ptr, v1[i].len).c_str());
    }
    hpcc_scalar_test_list_string_free(a);
}

void hpcc_scalar_test_list_test_list(hpcc_scalar_test_list_list_string_t *ret)
{
    ret->len = 4;
    ret->ptr = (hpcc_scalar_test_list_string_t *)malloc(ret->len * sizeof(hpcc_scalar_test_list_string_t));
    for (size_t i = 0; i < ret->len; ++i)
    {
        ret->ptr->len = 4;
        ret->ptr->ptr = (hpcc_scalar_test_string_t *)malloc(ret->ptr->len * sizeof(hpcc_scalar_test_string_t));
        for (size_t i = 0; i < ret->ptr->len; ++i)
        {
            std::string str = "test-";
            str += std::to_string(i);
            hpcc_scalar_test_string_dup(&ret->ptr->ptr[i], str.c_str());
        }
    }
}
