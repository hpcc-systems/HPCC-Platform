#ifndef CMCPP_FLOAT_HPP
#define CMCPP_FLOAT_HPP

#include "context.hpp"
#include "integer.hpp"

#include <cmath>

namespace cmcpp
{

    namespace float_
    {
        int32_t encode_float_as_i32(float32_t f);
        int64_t encode_float_as_i64(float64_t f);
        float32_t decode_i32_as_float(int32_t i);
        float64_t decode_i64_as_float(int64_t i);
        float32_t core_f32_reinterpret_i32(int32_t i);

        template <Float T>
        T canonicalize_nan(T f)
        {
            if (!std::isfinite(f))
            {
                f = std::numeric_limits<T>::quiet_NaN();
            }
            return f;
        }

        template <Float T>
        T maybe_scramble_nan(T f)
        {
            if (!std::isfinite(f))
            {
                f = std::numeric_limits<T>::quiet_NaN();
            }
            return f;
        }

        template <typename T>
        inline void store(CallContext &cx, const T &v, offset ptrnbytes)
        {
            cx.trap("store of unsupported type");
            throw std::runtime_error("trap not terminating execution");
        }

        template <>
        inline void store<float32_t>(CallContext &cx, const float32_t &v, offset ptr)
        {
            integer::store(cx, encode_float_as_i32(v), ptr);
        }

        template <>
        inline void store<float64_t>(CallContext &cx, const float64_t &v, offset ptr)
        {
            integer::store(cx, encode_float_as_i64(v), ptr);
        }

        template <typename T>
        T load(const CallContext &cx, offset ptr)
        {
            cx.trap("load of unsupported type");
            throw std::runtime_error("trap not terminating execution");
        }

        template <>
        inline float32_t load<float32_t>(const CallContext &cx, offset ptr)
        {
            return decode_i32_as_float(integer::load<int32_t>(cx, ptr));
        }

        template <>
        inline float64_t load<float64_t>(const CallContext &cx, offset ptr)
        {
            return decode_i64_as_float(integer::load<int64_t>(cx, ptr));
        }
    }
}

#endif
