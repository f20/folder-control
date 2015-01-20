#!/bin/sh

# Copyright 2014-2015 Franck Latrémolière, Reckon LLP.
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

if [ "$ZIP_FOLDER_NAME" == "" ]; then ZIP_FOLDER_NAME=Z_Zip; fi

unzipper ( ) {
    find "$1" -iname \*.zip | while read -r x
    do
        if [ "`echo "$x" | egrep '/(^|\/)(Z_|~\$)'`" == "" ]
        then
            y=`echo "$x" | sed -E 's/\.[zZ][iI][pP]$//'`
            while [ -e "$y" ]; do y="$y"_; done
            if mkdir "$y"
            then
                unzip -d "$y" -o "$x"
                z=`dirname "$x"`/"$ZIP_FOLDER_NAME"
                if [ ! -e "$z" ]; then mkdir "$z"; fi
                mv -n "$x" "$z"
                unzipper "$y"
            fi
        fi
    done
}

for r in "$@"
do
    unzipper "$r"
done
