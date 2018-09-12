package FileMgt106::ResolveFilter;

# Copyright 2018 Franck Latrémolière, Reckon LLP.
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

sub resolveAbsolutePaths {
    my ( $inputValue, $sha1FromStat, $sha1calc ) = @_;
    my ( $consolidated, $nonLinks );
    while ( my ( $k, $v ) = each %$inputValue ) {
        if ( ref $v eq 'HASH' ) {
            my ( $c, $n ) =
              resolveAbsolutePaths( $v, $sha1FromStat, $sha1calc );
            $consolidated->{$k} = $c if $c;
            $nonLinks->{$k}     = $n if $n;
        }
        elsif ( $v =~ m#^/.*?([^/]+)$#s && -f $v && -r _ ) {
            my $sha1 =
              $sha1FromStat->( $1, ( stat _ )[ 0, 1, 7, 9 ] );
            $sha1 = $sha1calc->($v) unless defined $sha1;
            $consolidated->{$k} = defined $sha1 ? unpack( 'H*', $sha1 ) : $v;
        }
        else {
            $consolidated->{$k} = $nonLinks->{$k} = $v;
        }
    }
    $consolidated, $nonLinks;
}

1;
