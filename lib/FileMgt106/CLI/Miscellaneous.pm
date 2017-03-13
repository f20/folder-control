package FileMgt106::CLI::Miscellaneous;

=head Copyright licence and disclaimer

Copyright 2011-2017 Franck Latrémolière, Reckon LLP.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY AUTHORS AND CONTRIBUTORS "AS IS" AND ANY
EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL AUTHORS OR CONTRIBUTORS BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

=cut

use warnings;
use strict;
use utf8;

sub simpleDedup {
    my ($hash) = @_;
    my ( %flagged, %new );
    foreach ( sort { length $a <=> length $b } keys %$hash ) {
        my $what = $hash->{$_};
        if ( ref $what eq 'HASH' ) {
            $new{$_} = simpleDedup($what);
        }
        elsif ( ref $what ) {
            $new{$_} = $what;
        }
        elsif ( !exists $flagged{$what} ) {
            $new{$_} = $what;
            undef $flagged{$what};
        }
    }
    \%new;
}

sub makeHintsFilterQuick {
    my ( $hints, $filterFlag ) = @_;
    my $searchSha1;
    $searchSha1 = $hints->{searchSha1} if $hints;
    my %done;
    my $filter;
    $filter = sub {
        my ($what) = @_;
        my $ref = ref $what;
        if ( $ref eq 'HASH' ) {
            my %h2;
            while ( my ( $k, $v ) = each %$what ) {
                $v = $filter->($v);
                $h2{$k} = $v if $v;
            }
            return keys %h2 ? \%h2 : undef;
        }
        elsif ( !$ref && $what && !$done{$what} ) {
            return unless $what =~ /([0-9a-zA-Z]{40})/;
            $done{$what} = 1;
            return
              if $searchSha1
              && $searchSha1->( pack( 'H*', $what ), 0 )->();
            return $what;
        }
        return;
    };
}

sub prettySize {
    my ($number) = @_;
    return int( 0.5 + $number * 1e-10 ) * 0.01 . 'T'
      if $number > 999_999_999_999;
    return int( 0.5 + $number * 1e-9 ) . 'G'        if $number > 99_999_999_999;
    return int( 0.5 + $number * 1e-8 ) * 0.1 . 'G'  if $number > 9_999_999_999;
    return int( 0.5 + $number * 1e-7 ) * 0.01 . 'G' if $number > 999_999_999;
    return int( 0.5 + $number * 1e-6 ) . 'M'        if $number > 99_999_999;
    return int( 0.5 + $number * 1e-5 ) * 0.1 . 'M'  if $number > 9_999_999;
    return int( 0.5 + $number * 1e-4 ) * 0.01 . 'M' if $number > 999_999;
    return int( 0.5 + $number * 1e-3 ) . 'k'        if $number > 99_999;
    return int( 0.5 + $number * 1e-2 ) * 0.1 . 'k'  if $number > 9_999;
    return int( 0.5 + $number * 1e-1 ) * 0.01 . 'k' if $number > 999;
    $number;
}

1;
