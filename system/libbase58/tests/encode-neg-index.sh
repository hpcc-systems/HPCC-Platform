#!/bin/sh
# This input causes the loop iteration counter to go negative
b58=$(echo '00CEF022FA' | xxd -r -p | base58)
test x$b58 = x16Ho7Hs
