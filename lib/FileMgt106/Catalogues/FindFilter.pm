package FileMgt106::Catalogues::FindFilter;

# Copyright 2021 Franck Latrémolière.
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

use strict;
use warnings;

sub processor {
    my ( $self, $regExp ) = @_;
    my $filter;
    $filter = sub {
        my ($hash) = @_;
        my %found;
        my $count = 0;
        foreach ( keys %$hash ) {
            my $w = $hash->{$_};
            if ( ref $w eq 'HASH' ) {
                my ( $nh, $cn ) = $filter->($w);
                $found{$_} = $nh if $cn;
                $count += $cn;
            }
            elsif ( defined $w && $w =~ /([0-9a-fA-F]{40})/ ) {
                if (/$regExp/) {
                    $found{$_} = $w;
                    ++$count;
                }
            }
        }
        \%found, $count;
    };
    my %consolidatedAdditions;
    sub {
        return keys %consolidatedAdditions ? \%consolidatedAdditions : ()
          unless @_;
        my ( $scalar, $path )  = @_;
        my ( $addh,   $count ) = $filter->($scalar);
        $path ||= 0;
        warn "$path: $count found.\n";
        $path =~ s^.*/^^s;
        $path .= '_' while exists $consolidatedAdditions{$path};
        $consolidatedAdditions{$path} = $addh if $count;
        return;
    };
}

1;
