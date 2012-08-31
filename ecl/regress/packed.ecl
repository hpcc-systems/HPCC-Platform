/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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


r := { packed unsigned8 value };
t := dataset([0, 1, 2, 10, 127, 128, 16383, 16384, 32767, 32768, 0xffffff, 0x7fffffff, 0xffffffff,
              0xffffffffffffff, 0x100000000000000, 0x7fffffffffffffff, 0xffffffffffffffff,
              (1<<1)-1, 1<<1,
              (1<<2)-1, 1<<2,
              (1<<3)-1, 1<<3,
              (1<<4)-1, 1<<4,
              (1<<5)-1, 1<<5,
              (1<<6)-1, 1<<6,
              (1<<7)-1, 1<<7,
              (1<<8)-1, 1<<8,
              (1<<9)-1, 1<<9,
              (1<<10)-1, 1<<10,
              (1<<11)-1, 1<<11,
              (1<<12)-1, 1<<12,
              (1<<13)-1, 1<<13,
              (1<<14)-1, 1<<14,
              (1<<15)-1, 1<<15,
              (1<<16)-1, 1<<16,
              (1<<17)-1, 1<<17,
              (1<<18)-1, 1<<18,
              (1<<19)-1, 1<<19,
              (1<<20)-1, 1<<20,
              (1<<21)-1, 1<<21,
              (1<<22)-1, 1<<22,
              (1<<23)-1, 1<<23,
              (1<<24)-1, 1<<24,
              (1<<25)-1, 1<<25,
              (1<<26)-1, 1<<26,
              (1<<27)-1, 1<<27,
              (1<<28)-1, 1<<28,
              (1<<29)-1, 1<<29,
              (1<<30)-1, 1<<30,
              (1<<31)-1, 1<<31,
              (1<<32)-1, 1<<32,
              (1<<33)-1, 1<<33,
              (1<<34)-1, 1<<34,
              (1<<35)-1, 1<<35,
              (1<<36)-1, 1<<36,
              (1<<37)-1, 1<<37,
              (1<<38)-1, 1<<38,
              (1<<39)-1, 1<<39,
              (1<<40)-1, 1<<40,
              (1<<41)-1, 1<<41,
              (1<<42)-1, 1<<42,
              (1<<43)-1, 1<<43,
              (1<<44)-1, 1<<44,
              (1<<45)-1, 1<<45,
              (1<<46)-1, 1<<46,
              (1<<47)-1, 1<<47,
              (1<<48)-1, 1<<48,
              (1<<49)-1, 1<<49,
              (1<<50)-1, 1<<50,
              (1<<51)-1, 1<<51,
              (1<<52)-1, 1<<52,
              (1<<53)-1, 1<<53,
              (1<<54)-1, 1<<54,
              (1<<55)-1, 1<<55,
              (1<<56)-1, 1<<56,
              (1<<57)-1, 1<<57,
              (1<<58)-1, 1<<58,
              (1<<59)-1, 1<<59,
              (1<<60)-1, 1<<60,
              (1<<61)-1, 1<<61,
              (1<<62)-1, 1<<62,
              (1<<63)-1, 1<<63,
              0], r);

output(t,,'out.d00',overwrite);
