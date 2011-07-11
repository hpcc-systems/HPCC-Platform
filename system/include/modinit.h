/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

// 
// Defines system wide MODULE_INIT priority values.
// The higher the value the earlier it will be created.
// Destruction is in reverse priority, i.e. highest will be destoyed last.
//
// Note: InitModuleObjects must be called asap in main body of each exe
//       (Although in Win32, objects will be initialized by standard OS dll dependency loading if not called).
//
// Note: ExitModuleObjects is called automatically by atexit or signal handlers on exit.



#define INIT_PRIORITY_SYSTEM         0xF000 // system, calls to system or non-dependent calls (can be called asap)


#define INIT_PRIORITY_JDEBUG1    INIT_PRIORITY_SYSTEM+0x900
#define INIT_PRIORITY_JHEAP      INIT_PRIORITY_SYSTEM+0x800
#define INIT_PRIORITY_JIFACE     INIT_PRIORITY_SYSTEM+0x800
#define INIT_PRIORITY_JPROP      INIT_PRIORITY_SYSTEM+0x800
#define INIT_PRIORITY_JTIME      INIT_PRIORITY_SYSTEM+0x800
#define INIT_PRIORITY_JHASH      INIT_PRIORITY_SYSTEM+0x800
#define INIT_PRIORITY_JPTREE     INIT_PRIORITY_SYSTEM+0x700

#define INIT_PRIORITY_JLOG       INIT_PRIORITY_SYSTEM+0x600
#define INIT_PRIORITY_JMISC1     INIT_PRIORITY_SYSTEM+0x500
#define INIT_PRIORITY_JDEBUG2    INIT_PRIORITY_SYSTEM+0x500
#define INIT_PRIORITY_JMISC2     INIT_PRIORITY_SYSTEM+0x400
#define INIT_PRIORITY_JTHREAD    INIT_PRIORITY_SYSTEM+0x300
#define INIT_PRIORITY_JBROADCAST INIT_PRIORITY_SYSTEM+0x200
#define INIT_PRIORITY_JFILE      INIT_PRIORITY_SYSTEM+0x200


#define INIT_PRIORITY_JLIB_DEPENDENT 0xE000 // requires module objects in jlib to have been initialized.

#define INIT_PRIORITY_DALI_DEPENDENT 0xD000 // requires module objects in dali to have been initalized.

#define INIT_PRIORITY_STANDARD       0x1000 // non inter dependent, can be initialized late, destroyed early
                                            // but might use jlib etc.


// These should be reviewed:
// Most if not all of these could use standard priority I suspect. Some *were* initialized before some jlib elements.
#define INIT_PRIORITY_HQLINTERNAL               INIT_PRIORITY_STANDARD+0x200        // before standard so hash table created.
#define INIT_PRIORITY_HQLMETA                   INIT_PRIORITY_STANDARD+0x280        // after atom, before internal
#define INIT_PRIORITY_HQLATOM                   INIT_PRIORITY_STANDARD+0x300        // before hqlinternal
#define INIT_PRIORITY_DEFTYPE                   INIT_PRIORITY_STANDARD+0x400        // before hqlinternal
#define INIT_PRIORITY_DEFVALUE                  INIT_PRIORITY_STANDARD+0x300        // before hqlinternal
#define INIT_PRIORITY_ENV_DALIENV               INIT_PRIORITY_STANDARD+100
#define INIT_PRIORITY_ENV_ENVIRONMENT           INIT_PRIORITY_STANDARD+150
#define INIT_PRIORITY_REMOTE_RMTFILE            INIT_PRIORITY_STANDARD+100
#define INIT_PRIORITY_JHTREE_JHTREE             INIT_PRIORITY_STANDARD+150
#define INIT_PRIORITY_ECLRTL_ECLRTL         INIT_PRIORITY_STANDARD+0x100
#define INIT_PRIORITY_MP_MPTAG                  INIT_PRIORITY_STANDARD+100

// No longer used, (for reference only)
#define INIT_PRIORITY_COMMONEXT                 50

#define INIT_PRIORITY_THOR_THDEMONSERVER        100
#define INIT_PRIORITY_THOR_THORPORT             100
#define INIT_PRIORITY_THOR_THSLAVEPROXY         100
#define INIT_PRIORITY_THOR_THORMISC             100

#define INIT_PRIORITY_DALI_DASDS                100
#define INIT_PRIORITY_DALI_DASESS               100
#define INIT_PRIORITY_DALI_DASERVER             100

