package FileMgt106::ConsolidateFilter;

=head Copyright licence and disclaimer

Copyright 2018 Franck Latrémolière.

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

use strict;
use warnings;

sub new {
    my ($class) = @_;
    bless {}, $class;
}

sub additionsProcessor {
    my ($seen) = @_;
    my $filter;
    $filter = sub {
        my ($hash) = @_;
        my %newHash;
        my $countNew = 0;
        my $countDup = 0;
        foreach ( keys %$hash ) {
            my $w = $hash->{$_};
            if ( ref $w eq 'HASH' ) {
                my ( $nh, $cn, $cd ) = $filter->($w);
                $newHash{$_} = $nh if $cn;
                $countNew += $cn;
                $countDup += $cd;
            }
            else {
                if ( exists $seen->{$w} ) {
                    ++$countDup;
                }
                else {
                    $newHash{$_} = $w;
                    ++$countNew;
                }
                undef $seen->{$w};
            }
        }
        \%newHash, $countNew, $countDup;
    };
    my %consolidatedAdditions;
    sub {
        unless (@_) {
            FileMgt106::LoadSave::saveJbz( "+consolidated-additions.jbz",
                \%consolidatedAdditions );
            return;
        }
        my ( $scalar, $path ) = @_;
        my ( $addh, $countNew, $countDup ) = $filter->($scalar);
        warn "$path processed: $countNew new, $countDup already seen.\n";
        $path =~ s^.*/^^s;
        $path .= '_' unless length $path;
        $path .= '_' while exists $consolidatedAdditions{$path};
        $consolidatedAdditions{$path} = $addh if $countNew;
        return;
    };
}

sub baseProcessor {
    my ($seen) = @_;
    my $preloader;
    $preloader = sub {
        my ($hash)   = @_;
        my $countNew = 0;
        my $countDup = 0;
        foreach ( values %$hash ) {
            if ( ref $_ eq 'HASH' ) {
                my ( $cn, $cd ) = $preloader->($_);
                $countNew += $cn;
                $countDup += $cd;
            }
            else {
                exists $seen->{$_} ? ++$countDup : ++$countNew;
                undef $seen->{$_};
            }
        }
        $countNew, $countDup;
    };
    sub {
        my ( $scalar, $path ) = @_ or return;
        my ( $countNew, $countDup ) = $preloader->($scalar);
        warn "$path loaded: $countNew new, $countDup duplicates.\n";
        return;
    };
}

1;
