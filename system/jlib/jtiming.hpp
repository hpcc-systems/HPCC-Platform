/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.

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



#ifndef JTIMING_HPP
#define JTIMING_HPP

#include "jiface.hpp"

#define TIMING

__int64 jlib_decl cycle_to_nanosec(cycle_t cycles);
__int64 jlib_decl cycle_to_microsec(cycle_t cycles);
__int64 jlib_decl cycle_to_millisec(cycle_t cycles);
cycle_t jlib_decl nanosec_to_cycle(__int64 cycles);
cycle_t jlib_decl millisec_to_cycle(unsigned ms);
double jlib_decl getCycleToNanoScale();
void jlib_decl display_time(const char * title, cycle_t diff);

// X86 / X86_64
#if defined(_ARCH_X86_64_) || defined(_ARCH_X86_)

#define HAS_GOOD_CYCLE_COUNTER

#if defined(_WIN32) && defined (_ARCH_X86_)
#pragma warning(push)
#pragma warning(disable:4035)
inline cycle_t getTSC() { __asm { __asm _emit 0x0f __asm _emit 0x31 } }
#pragma warning(pop)
#elif !defined(_WIN32)
inline cycle_t getTSC() { return __builtin_ia32_rdtsc(); }
#else
#include <intrin.h>
inline cycle_t getTSC() { return __rdtsc(); }
#endif // WIN32

#elif defined(_ARCH_PPC)

#define HAS_GOOD_CYCLE_COUNTER

static inline cycle_t getTSC()
{
    int64_t result;
#ifdef _ARCH_PPC64
    /*
        This reads timebase in one 64bit go.  Does *not* include a workaround for the cell (see 
        http://ozlabs.org/pipermail/linuxppc-dev/2006-October/027052.html)
    */
    __asm__ volatile(
        "mftb    %0"
        : "=r" (result));
#else
    /*
        Read the high 32bits of the timer, then the lower, and repeat if high order has changed in the meantime.  See
        http://ozlabs.org/pipermail/linuxppc-dev/1999-October/003889.html
    */
    unsigned long dummy;
    __asm__ volatile(
        "mfspr   %1,269\n\t"  /* mftbu */
        "mfspr   %L0,268\n\t" /* mftb */
        "mfspr   %0,269\n\t"  /* mftbu */
        "cmpw    %0,%1\n\t"   /* check if the high order word has chanegd */
        "bne     $-16"
        : "=r" (result), "=r" (dummy));
#endif
    return result;
}

#else
// ARMFIX: cycle-count is not always available in user mode
// http://infocenter.arm.com/help/index.jsp?topic=/com.arm.doc.ddi0338g/Bihbeabc.html
// http://neocontra.blogspot.co.uk/2013/05/user-mode-performance-counters-for.html
inline cycle_t getTSC() { return 0; }
#endif // X86

#if defined(INLINE_GET_CYCLES_NOW) && defined(HAS_GOOD_CYCLE_COUNTER)
inline cycle_t get_cycles_now() { return getTSC(); }
#else
cycle_t jlib_decl get_cycles_now();  // equivalent to getTSC when available
#endif

#endif