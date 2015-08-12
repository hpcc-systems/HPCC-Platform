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

potentialAlias(unsigned v1, unsigned v2) := (sqrt(v1 * v2));
unsigned forceAlias(unsigned v1, unsigned v2) := potentialAlias(v1, v2) * potentialAlias(v1, v2);

workRecord :=
            RECORD
unsigned        f1;
unsigned        f2;
unsigned        f3;
unsigned        f4;
unsigned        f5;
unsigned        f6;
unsigned        f7;
unsigned        f8;
unsigned        f9;
            END;

ds := dataset('ds', workRecord, thor);

workRecord t1(workRecord l) := TRANSFORM
    alias1 := forceAlias(l.f1, 1);
    alias2 := forceAlias(l.f1, 2);
    c1 := l.f2 = 100;
    c2 := l.f3 = 10;
    SELF.f1 := IF(c1, alias1, 102);
    SELF.f2 := IF(c2, alias1, 103);
    SELF := [];
END;

SEQUENTIAL(
    OUTPUT(PROJECT(ds, t1(LEFT))),
    'Done'
);


/*
Some sample idealized generated code:

a) unconditonal global

void transform(self, left)
{
    double v1 = rtlSqrt((double)(*((unsigned __int64 *)(left + 0)) * 1i64));
    __int64 v2 = (__int64)(unsigned __int64)(v1 * v1);
    if (left->f2 == 100)
        self->f1 = v2;
    else
        self->f1 = 102;
    if (left->f3 == 10)
        self->f2 = v2;
    else
        self->f2 = 102;
}

b) generate and evaluate where it is used

void transform(self, left)
{
    if (left->f2 == 100)
    {
        double v1 = rtlSqrt((double)(*((unsigned __int64 *)(left + 0)) * 1i64));
        __int64 v2 = (__int64)(unsigned __int64)(v1 * v1);
        self->f1 = v2;
    }
    else
        self->f1 = 102;
    if (left->f3 == 10)
    {
        double v1 = rtlSqrt((double)(*((unsigned __int64 *)(left + 0)) * 1i64));
        __int64 v2 = (__int64)(unsigned __int64)(v1 * v1);
        self->f2 = v2;
    }
    else
        self->f2 = 102;
}

c) generate as out of line functions

double a1(left)
{
    return rtlSqrt((double)(*((unsigned __int64 *)(left + 0)) * 1i64));
}
__int64 a2(left)
{
    double v = a1(left);
    return (__int64)(unsigned __int64)(v * v);
}
void transform(self, left)
{
    if (left->f2 == 100)
    {
        self->f1 = a2(left);
    }
    else
        self->f1 = 102;
    if (left->f3 == 10)
    {
        self->f2 = a2(left);
    }
    else
        self->f2 = 102;
}

d) generate as out of line classes protecting against re-evaluation

class f_a1 : public alias<double>
{
    // can't all be in the base class unless the parameters are all passed in the constructor - which is messy
    double evaluate(left)
    {
        if (!needToEvaluate()) // checks and sets a flag
            value = rtlSqrt((double)(*((unsigned __int64 *)(left + 0)) * 1i64));
        return value;
    }
}
class f_a2 : public alias<__int64>
{
    f_a2(f_a1 & _a1) : a1(_a1);
    __int64 evaluate(left, f_a2 & a1)
    {
        if (!needToEvaluate()) // checks and sets a flag
        {
            double v = a1(left);
            value = (__int64)(unsigned __int64)(v * v);
        }
        return value;
    }
private:
    f_a1 & a1;
}

void transform(self, left)
{
    f_a1 a1;
    f_a2 a2(a1);
    if (left->f2 == 100)
    {
        self->f1 = a2.evaluate(left);
    }
    else
        self->f1 = 102;
    if (left->f3 == 10)
    {
        self->f2 = a2.evaluate(left);
    }
    else
        self->f2 = 102;
}

e) generate globally with guard conditions:

void transform(self, left)
{
    bool c1 = (left->f2 == 100);
    bool c2 = (left->f3 == 10);
    //More: These could be improved - combined, or v1 only evaluated inside v2, but this would be the simplest to generate.
    double v1;
    if (c1 && c2)
        v1 = rtlSqrt((double)(*((unsigned __int64 *)(left + 0)) * 1i64));
    else
        v1 = 0;
    __int64 v2;
    if (c1 && c2)
        v2 = (__int64)(unsigned __int64)(v1 * v1);
    else
        v2 = 0;
    if (c1)
        self->f1 = v2;
    else
        self->f1 = 102;
    if (c2)
        self->f2 = v2;
    else
        self->f2 = 102;
}

f) with out of line function generation (v1):
double a1(left)
{
    return rtlSqrt((double)(*((unsigned __int64 *)(left + 0)) * 1i64));
}
__int64 a2(left)
{
    //NOTE: This would cause a1 to be unconditionally - re-evalauted if a1 was used from another context,
    //To avoid that a1 needs to be passed in as the parameter - see (g) below.
    double v = a1(left);
    return (__int64)(unsigned __int64)(v * v);
}
void transform(self, left)
{
    bool c1 = (left->f2 == 100);
    bool c2 = (left->f3 == 10);
    double v2;
    if (c1 && c2)
        v2 = a2(left);
    else
        v2 = 0;
    if (c1)
        self->f1 = v2;
    else
        self->f1 = 102;
    if (c2)
        self->f2 = v2;
    else
        self->f2 = 102;
}

g) with out of line function generation (v2):

double a1(left)
{
    return rtlSqrt((double)(*((unsigned __int64 *)(left + 0)) * 1i64));
}
__int64 a2(double v)
{
    return (__int64)(unsigned __int64)(v * v);
}
void transform(self, left)
{
    bool c1 = (left->f2 == 100);
    bool c2 = (left->f3 == 10);
    //More: These could be improved - combined, or v1 only evaluated inside v2, but this would be the simplest to generate.
    double v1;
    if (c1 && c2)
        v1 = a1(left);
    else
        v1 = 0;
    __int64 v2;
    if (c1 && c2)
        v2 = a2(v1 * v1);
    else
        v2 = 0;
    if (c1)
        self->f1 = v2;
    else
        self->f1 = 102;
    if (c2)
        self->f2 = v2;
    else
        self->f2 = 102;
}

*/
