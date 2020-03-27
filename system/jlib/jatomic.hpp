/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */


#ifndef JATOMIC_HPP
#define JATOMIC_HPP
#include "platform.h"

#include <atomic>

#ifdef _WIN32
inline static void spinPause() { YieldProcessor(); }
#elif defined(_ARCH_X86_64_) || defined(_ARCH_X86_)
# include "x86intrin.h"
# if defined(_ARCH_X86_)
inline static void spinPause() { __pause(); }
  // or could use
  // __asm__ __volatile__ ("rep; nop" ::: "memory");
  // __asm__ __volatile__ ("pause" ::: "memory");
# else
inline static void spinPause() { _mm_pause(); }
# endif
#elif defined(_ARCH_PPC64EL_)
inline static void spinPause() { } // MORE: Is there an equivalent?
#elif defined(_ARCH_ARM64_)
inline static void spinPause() { } // MORE: Is there an equivalent?
#else
inline static void spinPause() { }
#endif

template <typename T>
auto add_fetch(T & value, decltype(value.load()) delta, std::memory_order order = std::memory_order_seq_cst)  -> decltype(value.load()) { return value.fetch_add(delta, order) + delta; }
template <typename T>
auto sub_fetch(T & value, decltype(value.load()) delta, std::memory_order order = std::memory_order_seq_cst)  -> decltype(value.load()) { return value.fetch_sub(delta, order) - delta; }

//Use this class for stats which are gathered, but the values read from other threads do not need to be synchronized
//NOTE: Counts will never be lost, but the values read from another thread may be inconsistent.
//E.g., thread 1 updates x than y, thread 2 may read an updated value of y, but an old value of x.
template <typename T>
class RelaxedAtomic : public std::atomic<T>
{
public:
    typedef std::atomic<T> BASE;
    RelaxedAtomic() noexcept = default;
    inline constexpr RelaxedAtomic(T _value) noexcept : BASE(_value) { }
    ~RelaxedAtomic() noexcept = default;
    RelaxedAtomic(const RelaxedAtomic& _value) { BASE::store(_value.load()); }
    RelaxedAtomic& operator=(const RelaxedAtomic&) = delete;

    inline operator T() const noexcept { return load(); }
    inline T operator=(T _value) noexcept { store(_value); return _value; }
    inline T operator++() noexcept { return BASE::fetch_add(1, std::memory_order_relaxed)+1; }  // ++x
    inline T operator--() noexcept { return BASE::fetch_sub(1, std::memory_order_relaxed)-1; }  // --x
    inline T operator++(int) noexcept { return BASE::fetch_add(1, std::memory_order_relaxed); } // x++
    inline T operator--(int) noexcept { return BASE::fetch_sub(1, std::memory_order_relaxed); } // x--
    inline T operator+=(int v) noexcept { return BASE::fetch_add(v, std::memory_order_relaxed)+v; }
    inline T operator-=(int v) noexcept { return BASE::fetch_sub(v, std::memory_order_relaxed)-v; }

    inline void store(T _value, std::memory_order order = std::memory_order_relaxed) noexcept { BASE::store(_value, order); }
    inline T load(std::memory_order order = std::memory_order_relaxed) const noexcept { return BASE::load(order); }
    inline T exchange(T _value, std::memory_order order = std::memory_order_relaxed) noexcept { return BASE::exchange(_value, order); }
    inline T fetch_add(T _value, std::memory_order order = std::memory_order_relaxed) noexcept { return BASE::fetch_add(_value, order); }
    inline T fetch_sub(T _value, std::memory_order order = std::memory_order_relaxed) noexcept { return BASE::fetch_add(_value, order); }
    inline T add_fetch(T _value, std::memory_order order = std::memory_order_relaxed) noexcept { return ::add_fetch(*this, _value, order); }
    inline T sub_fetch(T _value, std::memory_order order = std::memory_order_relaxed) noexcept { return ::sub_fetch(*this, _value, order); }
    inline void store_max(T _value) noexcept { while (_value > load()) _value = BASE::exchange(_value, std::memory_order_acq_rel); }
    inline void store_min(T _value) noexcept { while (_value < load()) _value = BASE::exchange(_value, std::memory_order_acq_rel); }
};

// Class to accumulate values locally and only add atomically once

template <typename T>
class ScopedAtomic
{
public:
    inline ScopedAtomic(RelaxedAtomic<T> &_gval) : lval(0), gval(_gval) {}
    inline ~ScopedAtomic() { if (lval) gval.fetch_add(lval); }
    ScopedAtomic(const ScopedAtomic&) = delete;
    ScopedAtomic& operator=(const ScopedAtomic&) = delete;

    inline operator T() const noexcept { return lval; }
    inline T operator=(T _value) noexcept { lval = _value; return _value; }
    inline T operator++() noexcept { return ++lval; }
    inline T operator--() noexcept { return --lval; }
    inline T operator++(int) noexcept { return lval++; }
    inline T operator--(int) noexcept { return lval--; }
    inline T operator+=(int v) noexcept { return lval += v; }
    inline T operator-=(int v) noexcept { return lval -= v; }
private:
    T lval;
    RelaxedAtomic<T> &gval;
};

//Currently compare_exchange_weak in gcc forces a write to memory which is painful in highly contended situations.  The
//See https://gcc.gnu.org/bugzilla/show_bug.cgi?id=66867 for some details.  Marked as fixed for gcc 7.
//The symbol HAS_EFFICIENT_CAS should be defined if this bug is fixed, and/or there is no fallback implementation (e.g., windows)
//MCK verified gcc 7.1+ and all recent clang are ok
#if defined(_WIN32)
# define HAS_EFFICIENT_CAS
#elif defined(__GNUC__) && (__GNUC__ > 7 || (__GNUC__ == 7 && __GNUC_MINOR__ >= 1))
# define HAS_EFFICIENT_CAS
#elif defined(__clang__)
# define HAS_EFFICIENT_CAS
#endif

#if defined(HAS_EFFICIENT_CAS)

template <typename x>
bool compare_exchange_efficient(x & value, decltype(value.load()) & expected, decltype(value.load()) desired, std::memory_order order = std::memory_order_seq_cst)
{
    return value.compare_exchange_weak(expected, desired, order);
}

template <typename x>
bool compare_exchange_efficient(x & value, decltype(value.load()) & expected, decltype(value.load()) desired, std::memory_order successOrder = std::memory_order_seq_cst, std::memory_order failureOrder = std::memory_order_seq_cst)
{
    return value.compare_exchange_weak(expected, desired, successOrder, failureOrder);
}
#else
template <typename x>
//If HAS_EFFICIENT_CAS is not defined, the expected value is not updated => expected is not a reference
bool compare_exchange_efficient(x & value, decltype(value.load()) expected, decltype(value.load()) desired, std::memory_order order = std::memory_order_seq_cst)
{
    decltype(value.load()) * nastyCast = reinterpret_cast<decltype(value.load()) *>(&value);
    return __sync_bool_compare_and_swap(nastyCast, expected, desired);
}

template <typename x>
bool compare_exchange_efficient(x & value, decltype(value.load()) expected, decltype(value.load()) desired, std::memory_order successOrder = std::memory_order_seq_cst, std::memory_order failureOrder = std::memory_order_seq_cst)
{
    decltype(value.load()) * nastyCast = reinterpret_cast<decltype(value.load()) *>(&value);
    return __sync_bool_compare_and_swap(nastyCast, expected, desired);
}
#endif

#ifdef _WIN32

#include <intrin.h>

extern "C"
{
   LONG  __cdecl _InterlockedIncrement(LONG volatile *Addend);
   LONG  __cdecl _InterlockedDecrement(LONG volatile *Addend);
   LONG  __cdecl _InterlockedCompareExchange(LONG volatile * Dest, LONG Exchange, LONG Comp);
}

#pragma intrinsic (_InterlockedCompareExchange)
#define InterlockedCompareExchange _InterlockedCompareExchange
#pragma intrinsic (_InterlockedIncrement)
#define InterlockedIncrement _InterlockedIncrement
#pragma intrinsic (_InterlockedDecrement)
#define InterlockedDecrement _InterlockedDecrement
#pragma intrinsic (_InterlockedExchangeAdd)
#define InterlockedExchangeAdd _InterlockedExchangeAdd

typedef volatile long atomic_t;
#define ATOMIC_INIT(i)                  (i)
#define atomic_inc(v)                   InterlockedIncrement(v)
#define atomic_inc_and_test(v)          (InterlockedIncrement(v) == 0)
#define atomic_dec(v)                   InterlockedDecrement(v)
#define atomic_dec_and_test(v)          (InterlockedDecrement(v) == 0)
#define atomic_dec_and_read(v)           InterlockedDecrement(v)
#define atomic_read(v)                  (*v)
#define atomic_set(v,i)                 ((*v) = (i))
#define atomic_xchg(i, v)               InterlockedExchange(v, i)
#define atomic_add(v,i)                 InterlockedExchangeAdd(v,i)
#define atomic_add_and_read(v,i)        InterlockedAdd(v,i)
#define atomic_add_exchange(v, i)       InterlockedExchangeAdd(v,i)
#define atomic_xchg_ptr(p, v)           InterlockedExchangePointer(v,p)
#if defined (_MSC_VER) && (_MSC_VER <= 1200)
#define atomic_cas(v,newvalue,expectedvalue)    (InterlockedCompareExchange((PVOID *)(v),(PVOID)(long)(newvalue),(PVOID)(long)(expectedvalue))==(PVOID)(long)(expectedvalue))
#define atomic_cas_ptr(v, newvalue,expectedvalue)       atomic_cas(v,(long)newvalue,(long)expectedvalue)
#else
#define atomic_cas(v,newvalue,expectedvalue)    (InterlockedCompareExchange(v,newvalue,expectedvalue)==expectedvalue)
#define atomic_cas_ptr(v, newvalue,expectedvalue)       (InterlockedCompareExchangePointer(v,newvalue,expectedvalue)==expectedvalue)
#endif

//Used to prevent a compiler reordering volatile and non-volatile loads/stores
#define compiler_memory_barrier()           _ReadWriteBarrier()

#define atomic_acquire(v)               atomic_cas(v, 1, 0)
#define atomic_release(v)               { compiler_memory_barrier(); atomic_set(v, 0); }

#elif defined(__GNUC__)

typedef struct { volatile int counter; } atomic_t;
#define ATOMIC_INIT(i)          { (i) }
#define atomic_read(v)          ((v)->counter)
#define atomic_set(v,i)         (((v)->counter) = (i))

static __inline__ bool atomic_dec_and_test(atomic_t *v)
{   
    // returns (--*v==0)
    return (__sync_add_and_fetch(&v->counter,-1)==0);
}

static __inline__ bool atomic_inc_and_test(atomic_t *v)
{
    // returns (++*v==0)
    return (__sync_add_and_fetch(&v->counter,1)==0);
}

static __inline__ void atomic_inc(atomic_t *v)
{
    // (*v)++
    __sync_add_and_fetch(&v->counter,1);
}

static __inline__ void atomic_dec(atomic_t *v)
{
    // (*v)--
    __sync_add_and_fetch(&v->counter,-1);
}

static __inline__ int atomic_dec_and_read(atomic_t *v)
{
    // (*v)--, return *v;
    return __sync_add_and_fetch(&v->counter,-1);
}

static __inline__ int atomic_xchg(int i, atomic_t *v)
{
    // int ret = *v; *v = i; return v;
    return __sync_lock_test_and_set(&v->counter,i);  // actually an xchg
}



static __inline__ void atomic_add(atomic_t *v,int i)
{
    // (*v) += i;
    __sync_add_and_fetch(&v->counter,i);
}

static __inline__ int atomic_add_and_read(atomic_t *v,int i)
{
    // (*v) += i; return *v;
    return __sync_add_and_fetch(&v->counter,i);
}

static __inline__ int atomic_add_exchange(atomic_t *v,int i)
{
    // int ret = *v; (*v) += i; return ret;
    return __sync_fetch_and_add(&v->counter,i);
}

static __inline__ bool atomic_cas(atomic_t *v,int newvalue, int expectedvalue)
{
    // bool ret = (*v==expectedvalue); if (ret) *v = newvalue; return ret;
    return __sync_bool_compare_and_swap(&v->counter, expectedvalue, newvalue);
}

static __inline__ void * atomic_xchg_ptr(void *p, void **v)
{
    // void * ret = *v; (*v) = p; return ret;
    return (void *)__sync_lock_test_and_set((memsize_t *)v,(memsize_t)p);
}

static __inline__ bool atomic_cas_ptr(void **v,void *newvalue, void *expectedvalue)
{
    // bool ret = (*v==expectedvalue); if (ret) *v = newvalue; return ret;
    return __sync_bool_compare_and_swap((memsize_t *)v, (memsize_t)expectedvalue, (memsize_t)newvalue);
}

#define compiler_memory_barrier() asm volatile("": : :"memory")

static __inline__ bool atomic_acquire(atomic_t *v)
{
#if defined(_ARCH_X86_64_) || defined(_ARCH_X86_)
    //For some reason gcc targeting x86 generates code for atomic_cas() that requires fewer registers
    return atomic_cas(v, 1, 0);
#else
    return __sync_lock_test_and_set(&v->counter, 1) == 0;
#endif
}

static __inline__ void atomic_release(atomic_t *v)
{
#if defined(_ARCH_X86_64_) || defined(_ARCH_X86_)
    //x86 has a strong memory model, so the following code is sufficient, and some older gcc compilers generate
    //an unnecessary mfence instruction, so for x86 use the following which generates better code.
    compiler_memory_barrier();
    atomic_set(v, 0);
#else
    __sync_lock_release(&v->counter);
#endif
}

#else // other unix

//Truely awful implementations of atomic operations...
typedef volatile int atomic_t;
int jlib_decl poor_atomic_dec_and_read(atomic_t * v);
bool jlib_decl poor_atomic_inc_and_test(atomic_t * v);
int jlib_decl poor_atomic_xchg(int i, atomic_t * v);
void jlib_decl poor_atomic_add(atomic_t * v, int i);
int jlib_decl poor_atomic_add_and_read(atomic_t * v, int i);
int jlib_decl poor_atomic_add_exchange(atomic_t * v, int i);
bool jlib_decl poor_atomic_cas(atomic_t * v, int newvalue, int expectedvalue);
void jlib_decl *poor_atomic_xchg_ptr(void *p, void **v);
bool   jlib_decl poor_atomic_cas_ptr(void ** v, void *newvalue, void *expectedvalue);
void jlib_decl poor_compiler_memory_barrier();

#define ATOMIC_INIT(i)                  (i)
#define atomic_inc(v)                   (void)poor_atomic_inc_and_test(v)
#define atomic_inc_and_test(v)          poor_atomic_inc_and_test(v)
#define atomic_dec(v)                   (void)poor_atomic_dec_and_read(v)
#define atomic_dec_and_read(v)          poor_atomic_dec_and_read(v)
#define atomic_dec_and_test(v)          (poor_atomic_dec_and_read(v)==0)
#define atomic_read(v)                  (*v)
#define atomic_set(v,i)                 ((*v) = (i))
#define atomic_xchg(i, v)               poor_atomic_xchg(i, v)
#define atomic_add(v,i)                 poor_atomic_add(v, i)
#define atomic_add_and_read(v,i)        poor_atomic_add_and_read(v, i)
#define atomic_add_exchange(v, i)       poor_atomic_add_exchange(v, i)
#define atomic_cas(v,newvalue,expectedvalue)    poor_atomic_cas(v,newvalue,expectedvalue)
#define atomic_xchg_ptr(p, v)               poor_atomic_xchg_ptr(p, v)
#define atomic_cas_ptr(v,newvalue,expectedvalue)    poor_atomic_cas_ptr(v,newvalue,expectedvalue)
#define compiler_memory_barrier()       poor_compiler_memory_barrier()

#define atomic_acquire(v)               atomic_cas(v, 1, 0)
#define atomic_release(v)               { compiler_memory_barrier(); atomic_set(v, 0); }

#endif


#endif
