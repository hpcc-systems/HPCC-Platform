#!/bin/sh
hex=$(echo 993233 | xxd -r -p | base58 -d 25 || echo FAIL)
test "x${hex}" = "xFAIL"
