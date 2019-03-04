package FileMgt106::FolderClean;

# Copyright 2011-2019 Franck Latrémolière, Reckon LLP.
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

use warnings;
use strict;

use constant { STAT_NLINK => 3, };

sub deepClean {
    my $count = 0;
    foreach my $folder (@_) {
        my @list;
        my $dh;
        opendir $dh, $folder or next;
        @list = sort {
            (
                  $b =~ /^(?:\~?\$|Z?_)/s ? "X $b"
                : $b =~ /\.tmp$/si        ? "T $b"
                : $b =~ /^Y_.* folder$/s  ? "B $b"
                :                           "E $b"
              ) cmp(
                $a =~ /^(?:\~?\$|Z?_)/s  ? "X $a"
                : $a =~ /\.tmp$/si       ? "T $a"
                : $a =~ /^Y_.* folder$/s ? "B $a"
                :                          "E $a"
              )
        } grep { !/^\.\.?$/s } readdir $dh;
        closedir $dh;
        foreach my $file (@list) {
            if ( $file eq '.git' ) {
                ++$count;
                next;
            }
            my $fullPath = "$folder/$file";
            if ( $file =~ /^(?:\.|\:2e)(?:DS_Store$|_)/ ) {
                unlink $fullPath;
                next;
            }
            my $newPath;
            $newPath = "$folder/_$file" if $file =~ /^(\~\$|Z_|\.)/is;
            $newPath = "$folder/${file}_"
              if $file =~ /\.(?:app|aplibrary|download|lrcat|lrdata|tmp)$/is;
            if ($newPath) {
                if ( -e $newPath ) {
                    my ( $base, $extension ) =
                      ( $newPath =~ m#(.*)(\.[^ /]+)$#s );
                    ( $base, $extension ) = ( $newPath, '' )
                      unless defined $extension;
                    my $c = 2;
                    while ( -e ( $newPath = "$base~$c$extension" ) ) { ++$c; }
                }
                $fullPath = $newPath if rename $fullPath, $newPath;
            }
            my $nlinks = ( lstat $fullPath )[STAT_NLINK];
            ++$count
              unless -d _ and !deepClean($fullPath) and rmdir $fullPath
              or -l _ || -z _ and unlink $fullPath
              or -f _
              and $nlinks > 1
              and unlink $fullPath;
        }
    }
    $count;
}

1;
