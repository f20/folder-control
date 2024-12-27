package FileMgt106::Catalogues::ConsolidateFilter;

# Copyright 2018-2024 Franck Latrémolière.
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
    bless {}, $class;
}

sub baseProcessor {
    my ($seen) = @_;
    my $preloader;
    $preloader = sub {
        my ($hash)      = @_;
        my $countUnseen = 0;
        my $countSeen   = 0;
        foreach ( values %$hash ) {
            if ( ref $_ eq 'HASH' ) {
                my ( $cUnseen, $cSeen ) = $preloader->($_);
                $countUnseen += $cUnseen;
                $countSeen   += $cSeen;
            }
            elsif ( defined $_ && /([0-9a-fA-F]{40})/ ) {
                my $sha1 = lc $1;
                exists $seen->{$sha1} ? ++$countSeen : ++$countUnseen;
                undef $seen->{$sha1};
            }
        }
        $countUnseen, $countSeen;
    };
    sub {
        my ( $scalar,      $path )      = @_ or return;
        my ( $countUnseen, $countSeen ) = $preloader->($scalar);
        warn "$path: $countUnseen new, $countSeen duplicated.\n";
        return;
    };
}

sub unseenProcessor {
    my ($seen) = @_;
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
                if ( exists $seen->{$sha1} ) {
                    ++$countSeen;
                }
                else {
                    $newHash{$_} = $w;
                    ++$countUnseen;
                }
                undef $seen->{$sha1};
            }
        }
        \%newHash, $countUnseen, $countSeen;
    };
    my %consolidationResult;
    sub {
        return keys %consolidationResult ? \%consolidationResult : ()
          unless @_;
        my ( $scalar, $path ) = @_;
        my ( $addh, $countUnseen, $countSeen ) = $filter->($scalar);
        $path ||= 0;
        warn "$path: $countUnseen new, $countSeen already seen.\n";
        $path =~ s^.*/^^s;
        $path .= '_' while exists $consolidationResult{$path};
        $consolidationResult{$path} = $addh if $countUnseen;
        return;
    };
}

sub seenProcessor {
    my ($seen) = @_;
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
                $newHash{$_} = $nh if $cSeen;
                $countUnseen += $cUnseen;
                $countSeen   += $cSeen;
            }
            elsif ( defined $w && $w =~ /([0-9a-fA-F]{40})/ ) {
                my $sha1 = lc $1;
                if ( exists $seen->{$sha1} ) {
                    $newHash{$_} = $w;
                    ++$countSeen;
                }
                else {
                    ++$countUnseen;
                }
            }
        }
        \%newHash, $countUnseen, $countSeen;
    };
    my %consolidationResult;
    sub {
        return keys %consolidationResult ? \%consolidationResult : () unless @_;
        my ( $scalar, $path ) = @_;
        my ( $addh, $countUnseen, $countSeen ) = $filter->($scalar);
        $path ||= 0;
        warn "$path: $countSeen already seen, $countUnseen not seen before.\n";
        $path =~ s^.*/^^s;
        $path .= '_' while exists $consolidationResult{$path};
        $consolidationResult{$path} = $addh if $countSeen;
        return;
    };
}

sub duplicationsByPairProcessor {
    my %objectsToExamine;
    sub {
        if ( my ( $scalar, $path ) = @_ ) {
            $path =~ s^.*/^^s;
            $path =~ s/\.(?:txt|json|jbz)$//si;
            $path .= '_' while exists $objectsToExamine{$path};
            $objectsToExamine{$path} = $scalar;
            return;
        }
        else {
            my %results;
            my %doNotBother;
            foreach my $a ( keys %objectsToExamine ) {
                foreach my $b ( keys %objectsToExamine ) {
                    next if $a eq $b;
                    next if exists $doNotBother{$a}{$b};
                    my $consolidator =
                      FileMgt106::Catalogues::ConsolidateFilter->new;
                    $consolidator->baseProcessor->( $objectsToExamine{$a}, $a );
                    my $processor = $consolidator->seenProcessor;
                    $processor->( $objectsToExamine{$b}, $b );
                    if ( my $dups = $processor->() ) {
                        while ( my ( $k, $v ) = each %$dups ) {
                            $results{"Duplicated from $a"}{$k} = $v;
                        }
                    }
                    else {
                        undef $doNotBother{$b}{$a};
                    }
                }
            }
            keys %results ? \%results : ();
        }
    };
}

sub consolidateProcessor {
    my $runner;
    $runner = sub {
        my ( $accumulator, $additions ) = @_;
        while ( my ( $k, $v ) = each %$additions ) {
            unless ( $accumulator->{$k} ) {
                $accumulator->{$k} = $v;
                next;
            }
            if ( 'HASH' eq ref $accumulator->{$k} && 'HASH' eq ref $v ) {
                $runner->( $accumulator->{$k}, $v );
                next;
            }
            next if lc $accumulator->{$k} eq lc $v;
            if ( $v =~ /\// ) {
                warn "Ignored for $k: $v, keeping $accumulator->{$k}\n";
              next:
            }
            if ( $accumulator->{$k} =~ /\// ) {
                warn "Repaired for $k: $v, replacing $accumulator->{$k}\n";
                $accumulator->{$k} = $v;
                next;
            }
            warn "Conflict for $k: $accumulator->{$k}, $v\n";
            my ( $base, $extension ) = ( $k =~ m#^(.*?)(\.[a-zA-Z]?\S*)$#s );
            ( $base, $extension ) = ( $k, '' ) unless defined $extension;
            $base .= '~';
            my $number = 1;
            my $k2;
            do { $k2 = $base . sprintf( '%02d', ++$number ) . $extension; }
              while exists $accumulator->{$k2};
            $accumulator->{$k2} = $v;
        }
    };
    my $consolidationResult = {};
    sub {
        my ( $scalar, $path ) = @_ or return $consolidationResult;
        my $accumulator = $consolidationResult;
        if ( defined $path ) {
            my @segments = split / \$/, $path;
            shift @segments;
            $accumulator = $accumulator->{$_} ||= {} foreach @segments;
        }
        $runner->( $accumulator, $scalar );
        return;
    };
}

1;
