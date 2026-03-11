#pragma once

// Messages sent over mpTag for keyed-limit coordination.
enum class KeyedLimitMsg : unsigned char
{
    None = 0,
    Progress = 1,
    Done = 2,
    Abort = 3
};
