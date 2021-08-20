package FileMgt106::Catalogues::IntersectFilter;

# Copyright 2021 Franck LatrŽmolire.
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

sub new {
    my ($class) = @_;
    my @tags;
    my %bitmap;
    my @counter;
    my %firstSeen;
    my $self = bless [ \@tags, \%bitmap, \@counter, \%firstSeen ], $class;
}

sub result {
    my ($self) = @_;
    my ( $tags, $bitmap, $counter, $firstSeen ) = @$self;
    my ( $mask, $filter );
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
                if ( $bitmap->{ lc $1 } == $mask ) {
                    $found{$_} = $w;
                    ++$count;
                }
            }
        }
        \%found, $count;
    };
    for ( $mask = 1 ; $mask < 1 << @$tags ; ++$mask ) {
        next unless $counter->[$mask];
        my ( $found, $count ) = $filter->($firstSeen);
        FileMgt106::Catalogues::LoadSaveNormalize::saveJbz(
            (
                join ' ',
                (
                    map { $mask & ( 1 << $_ ) ? $tags->[$_] : (); }
                      0 .. $#$tags
                ),
                ( unpack 'b' . @$tags, pack 'L', $mask ),
            )
            . '.jbz',
            $found
        );
    }
}

sub taggedProcessor {
    my ( $self, $tag, $addFlag ) = @_;
    my ( $tags, $bitmap, $counter, $firstSeen ) = @$self;
    my $mask = 0;
    if ($tag) {
        my $tagid = 0;
        ++$tagid while ( $tag ne ( $tags->[$tagid] //= $tag ) );
        $mask = 1 << $tagid;
    }
    my $filter;
    $filter = sub {
        my ($hash) = @_;
        my %newHash;
        my $countUnseen = 0;
        my $countSeen   = 0;
        foreach ( keys %$hash ) {
            my $w = $hash->{$_};
            if ( ref $w eq 'HASH' ) {
                my ( $nh, $cUnseen, $cSeen ) = $filter->($w);
                $newHash{$_} = $nh if $cUnseen;
                $countUnseen += $cUnseen;
                $countSeen   += $cSeen;
            }
            elsif ( defined $w && $w =~ /([0-9a-fA-F]{40})/ ) {
                my $sha1 = lc $1;
                if ( exists $bitmap->{$sha1} ) {
                    ++$countSeen;
                    --$counter->[ $bitmap->{$sha1} ];
                    ++$counter->[ $bitmap->{$sha1} |= $mask ];
                }
                else {
                    ++$countUnseen;
                    if ($addFlag) {
                        $newHash{$_} = $w;
                        ++$counter->[ $bitmap->{$sha1} = $mask ];
                    }
                }
            }
        }
        \%newHash, $countUnseen, $countSeen;
    };
    sub {
        return $self->result unless @_;
        my ( $scalar, $path ) = @_;
        my ( $addh, $countUnseen, $countSeen ) = $filter->($scalar);
        $path ||= 0;
        warn "$path: $countSeen already seen, $countUnseen not seen before.\n";
        $path =~ s^.*/^^s;
        $path .= '_' while exists $firstSeen->{$path};
        $firstSeen->{$path} = $addh if $countUnseen;
        return;
    };
}

1;
