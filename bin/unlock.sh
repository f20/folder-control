#!/bin/sh

# Copyright 2016-2019 Franck Latrémolière, Reckon LLP.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY AUTHORS AND CONTRIBUTORS "AS IS" AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL AUTHORS OR CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
# THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

if [ "$*" != "" ]
then
	find "$@" -type f -not -name .DS_Store -links 1 -not -perm -0220 -exec chmod ug+w {} \;
	find "$@" -type f -not -name .DS_Store -not -links 1 -perm -0220 -exec chmod ug-w {} \;
	find "$@" -type f -not -name .DS_Store -not -perm -0220 | while read FILE
    do
        echo "$FILE: unlocking by duplication"
        cp "$FILE" "$FILE$$"
        chmod ug+w "$FILE$$"
        if [ -x "$FILE" ]; then chmod +x "$FILE$$"; fi
        mv -f "$FILE$$" "$FILE"
    done
fi
