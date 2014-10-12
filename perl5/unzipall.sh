#!/usr/bin/env bash

# Copyright 2014 Franck Latrémolière, Reckon LLP.
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

shopt -s nocasematch
l1=("$@")
while [[ "${l1[@]}" != "" ]]
do
    l2=()
    for r in "${l1[@]}"
    do
        if pushd "$r"
        then
            while read -r -u3 x
            do
                if [[ ! "$x" =~ /(Z_|~\$) ]]
                then
                    y=${x/\.zip/}
                    while [ -e "$y" ]; do y="$y"_; done
                    if mkdir "$y"
                    then
                        cd "$y"
                        l2+=("`pwd`")
                        unzip -o  "$x"
                        z=`dirname "$x"`/Z_Zip
                        if [ ! -e "$z" ]; then mkdir "$z"; fi
                        mv -n "$x" "$z"
                    fi
                fi
            done 3< <(find "`pwd`" -iname \*.zip)
            popd
        else
            echo "Cannot pushd: $r"
        fi
    done
    l1=("${l2[@]}")
done
