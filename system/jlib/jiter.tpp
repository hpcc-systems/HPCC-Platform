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


#ifndef CCLITER_TPP
#define CCLITER_TPP

template <class BASE>
void NullIteratorOf<BASE>::First(void) { return false; }

template <class BASE>
bool NullIteratorOf<BASE>::First() { return false; }

template <class BASE>
bool NullIteratorOf<BASE>::Next() { return false; }

template <class BASE>
bool NullIteratorOf<BASE>::IsValid() { return false; }

template <class BASE>
BASE & NullIteratorOf<BASE>::Get() { assertex(!"Element not valid"); return *(BASE *)0; }

template <class BASE>
bool NullIteratorOf<BASE>::Last() { return false; }

template <class BASE>
bool NullIteratorOf<BASE>::Prev() { return false; }

template <class BASE>
bool NullIteratorOf<BASE>::Select(unsigned) { return false; }

#endif
