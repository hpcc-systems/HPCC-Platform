#include "traits.hpp"
#include "lift.hpp"
#include "lower.hpp"
#include "util.hpp"
#include "host-util.hpp"

using namespace cmcpp;

#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <iostream>
#include <vector>
#include <utility>
#include <cassert>
// #include <fmt/core.h>

class Heap
{
private:
    uint32_t last_alloc = 0;

public:
    std::vector<uint8_t> memory;

    Heap(size_t arg) : memory(arg), last_alloc(0)
    {
        CHECK(true);
    }

    Heap() : memory(1024 * 1024), last_alloc(0)
    {
        CHECK(true);
    }

    uint32_t realloc(uint32_t original_ptr, size_t original_size, uint32_t alignment, size_t new_size)
    {
        if (original_ptr != 0 && new_size < original_size)
        {
            return align_to(original_ptr, alignment);
        }
        uint32_t ret = align_to(last_alloc, alignment);
        last_alloc = ret + new_size;
        if (last_alloc > memory.size())
        {
            std::cout << "oom: have " << memory.size() << " need " << last_alloc << std::endl;
            trap("oom");
        }
        std::memcpy(&memory[ret], &memory[original_ptr], original_size);
        return ret;
    }
};

std::unique_ptr<CallContext> createCallContext(Heap *heap, Encoding encoding)
{
    std::unique_ptr<cmcpp::InstanceContext> instanceContext = std::make_unique<cmcpp::InstanceContext>(trap, convert,
        [heap](int original_ptr, int original_size, int alignment, int new_size) -> int
        {
            return heap->realloc(original_ptr, original_size, alignment, new_size);
        });
    return instanceContext->createCallContext(heap->memory, encoding);
}

TEST_CASE("Boolean")
{
    Heap heap;
    auto cx = createCallContext(&heap, Encoding::Utf8);
    auto v = lower_flat(*cx, true);
    auto b = lift_flat<bool_t>(*cx, v);
    CHECK(b == true);
    v = lower_flat(*cx, false);
    b = lift_flat<bool_t>(*cx, v);
    CHECK(b == false);
}

template <Numeric T>
void test_numeric(const std::unique_ptr<CallContext> &cx, T v = 42)
{
    auto flat_v = lower_flat(*cx, v);
    auto b = lift_flat<T>(*cx, flat_v);
    CHECK(b == v);
    v = ValTrait<T>::LOW_VALUE;
    flat_v = lower_flat(*cx, v);
    b = lift_flat<T>(*cx, flat_v);
    CHECK(b == v);
    v = ValTrait<T>::HIGH_VALUE;
    flat_v = lower_flat(*cx, v);
    b = lift_flat<T>(*cx, flat_v);
    CHECK(b == v);
}

TEST_CASE("Signed Integer")
{
    Heap heap;
    auto cx = createCallContext(&heap, Encoding::Utf8);
    test_numeric<int8_t>(cx);
    test_numeric<int16_t>(cx);
    test_numeric<int32_t>(cx);
    test_numeric<int64_t>(cx);
    test_numeric<int8_t>(cx, -42);
    test_numeric<int16_t>(cx, -42);
    test_numeric<int32_t>(cx, -42);
    test_numeric<int64_t>(cx, -42);
}

TEST_CASE("Unigned Integer")
{
    Heap heap;
    auto cx = createCallContext(&heap, Encoding::Utf8);
    test_numeric<uint8_t>(cx);
    test_numeric<uint16_t>(cx);
    test_numeric<uint32_t>(cx);
    test_numeric<uint64_t>(cx);
}

TEST_CASE("Float")
{
    Heap heap;
    auto cx = createCallContext(&heap, Encoding::Utf8);
    test_numeric<float32_t>(cx);
    test_numeric<float64_t>(cx);
    test_numeric<float32_t>(cx, -42);
    test_numeric<float64_t>(cx, -42);
}

const char *const hw = "hello world";
TEST_CASE("String")
{
    Heap heap;
    auto cx = createCallContext(&heap, Encoding::Utf8);
    auto v = lower_flat(*cx, string_t{Encoding::Utf8, (const char8_t *)hw, strlen(hw)});
    auto str = lift_flat<string_t>(*cx, v);
    CHECK(str.encoding == Encoding::Utf8);
    CHECK(str.byte_len == strlen(hw));
    CHECK(std::string((const char *)str.ptr, str.byte_len) == hw);
}

TEST_CASE("List")
{
    Heap heap;
    auto cx = createCallContext(&heap, Encoding::Utf8);
    list_t<string_t> strings = {string_t{Encoding::Utf8, (const char8_t *)hw, 5}, string_t{Encoding::Utf8, (const char8_t *)hw, 3}};
    auto v = lower_flat(*cx, strings);
    auto strs = lift_flat<list_t<string_t>>(*cx, v);
    CHECK(strs.size() == 2);
    CHECK(strs[0].encoding == Encoding::Utf8);
    CHECK(strs[0].byte_len == 5);
    CHECK(std::string((const char *)strs[0].ptr, strs[0].byte_len) == std::string(hw, strs[0].byte_len));
    CHECK(strs[1].encoding == Encoding::Utf8);
    CHECK(strs[1].byte_len == 3);
    CHECK(std::string((const char *)strs[1].ptr, strs[1].byte_len) == std::string(hw, strs[1].byte_len));
    v = lower_flat(*cx, strings);
    strs = lift_flat<list_t<string_t>>(*cx, v);
    CHECK(strs.size() == 2);
    CHECK(strs[0].encoding == Encoding::Utf8);
    CHECK(strs[0].byte_len == 5);
    CHECK(std::string((const char *)strs[0].ptr, strs[0].byte_len) == std::string(hw, strs[0].byte_len));
    CHECK(strs[1].encoding == Encoding::Utf8);
    CHECK(strs[1].byte_len == 3);
    CHECK(std::string((const char *)strs[1].ptr, strs[1].byte_len) == std::string(hw, strs[1].byte_len));
}

TEST_CASE("List2")
{
    Heap heap;
    auto cx = createCallContext(&heap, Encoding::Utf8);
    list_t<list_t<string_t>> strings = {{string_t{Encoding::Utf8, (const char8_t *)hw, 5}, string_t{Encoding::Utf8, (const char8_t *)hw, 3}}, {string_t{Encoding::Utf8, (const char8_t *)hw, 5}, string_t{Encoding::Utf8, (const char8_t *)hw, 3}}, {string_t{Encoding::Utf8, (const char8_t *)hw, 5}, string_t{Encoding::Utf8, (const char8_t *)hw, 3}}};
    auto v = lower_flat(*cx, strings);
    auto strs = lift_flat<list_t<list_t<string_t>>>(*cx, v);
    CHECK(strs.size() == 3);
    CHECK(strs[0][0].encoding == Encoding::Utf8);
    CHECK(strs[0][0].byte_len == 5);
    CHECK(std::string((const char *)strs[0][0].ptr, strs[0][0].byte_len) == std::string(hw, strs[0][0].byte_len));
    CHECK(strs[1][0].encoding == Encoding::Utf8);
    CHECK(strs[1][0].byte_len == 5);
    CHECK(std::string((const char *)strs[0][1].ptr, strs[0][1].byte_len) == std::string(hw, strs[0][1].byte_len));
    v = lower_flat(*cx, strings);
    strs = lift_flat<list_t<list_t<string_t>>>(*cx, v);
    CHECK(strs.size() == 3);
    CHECK(strs[0][0].encoding == Encoding::Utf8);
    CHECK(strs[0][0].byte_len == 5);
    CHECK(std::string((const char *)strs[0][0].ptr, strs[0][0].byte_len) == std::string(hw, strs[0][0].byte_len));
    CHECK(strs[1][0].encoding == Encoding::Utf8);
    CHECK(strs[1][0].byte_len == 5);
    CHECK(std::string((const char *)strs[0][1].ptr, strs[0][1].byte_len) == std::string(hw, strs[0][1].byte_len));
}

struct MyRecord0 {
    uint16_t age;
    uint32_t weight;
};

TEST_CASE("Records")
{
    Heap heap;
    auto cx = createCallContext(&heap, Encoding::Utf8);

    using R0 = record_t<uint16_t, uint32_t>;
    R0 r0 = {42, 43};
    auto v = lower_flat(*cx, r0);
    auto rr = lift_flat<R0>(*cx, v);
    CHECK(r0 == rr);
    auto rr0 = to_struct<MyRecord0>(rr);
    // CHECK(r2.age == rr0.age);
    // CHECK(r2.weight == rr0.weight);
}
