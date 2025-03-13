#include "float.hpp"

namespace cmcpp
{
    namespace float_
    {
        int32_t encode_float_as_i32(float32_t f)
        {
            return *reinterpret_cast<int32_t*>(&f);
        }

        int64_t encode_float_as_i64(float64_t f)
        {
            return *reinterpret_cast<int64_t*>(&f);
        }

        float32_t decode_i32_as_float(int32_t i)
        {
            return *reinterpret_cast<float32_t*>(&i);
        }

        float64_t decode_i64_as_float(int64_t i)
        {
            return *reinterpret_cast<float64_t*>(&i);
        }

        float32_t core_f32_reinterpret_i32(int32_t i)
        {
            float f;
            std::memcpy(&f, &i, sizeof f);
            return f;
        }
    }
}
