package FileMgt106::Catalogues::HintsFilter;

# Copyright 2011-2020 Franck Latrémolière, Reckon LLP and others.
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
use FileMgt106::Database;
use FileMgt106::FileSystem qw(STAT_DEV STAT_MODE STAT_UID);

sub makeHintsFilter {

    my ( $hintsFile, $devNo, $devOnly ) = @_;

    my $hints = FileMgt106::Database->new( $hintsFile, 1 );
    my $searchSha1 = $hints->{searchSha1};
    my %seen;
    my $sha1Machine;
    require Digest::SHA;
    $sha1Machine = new Digest::SHA;

    my $filterTree;
    $filterTree = sub {
        my ($sourceValue) = @_;
        my $returnValue;

      ENTRY: while ( my ( $name, $what ) = each %$sourceValue ) {
            next if $name =~ m#/#;
            if ( ref $what ) {
                if ( ref $what eq 'HASH' ) {
                    my $rv = $filterTree->( $what, $devNo );
                    $returnValue->{$name} = $rv if $rv;
                }
                next;
            }
            next if exists $seen{$what};
            undef $seen{$what};
            next unless $what =~ /([0-9a-fA-F]{40})/;
            my $sha1 = pack( 'H*', $1 );
            my $iterator = $searchSha1->( $sha1, $devNo );
            my ( @stat, @candidates, @reservelist );
            while ( !@stat
                && ( my ( $path, $statref, $locid ) = $iterator->() ) )
            {
                next unless -f _;
                last if $devOnly && $statref->[STAT_DEV] != $devNo;
                if (
                    !$locid
                    || ( $statref->[STAT_UID]
                        && ( $statref->[STAT_MODE] & 0200 ) )
                    || ( $statref->[STAT_MODE] & 022 )
                  )
                {
                    push @reservelist, $path;
                    next;
                }
                next ENTRY;
            }
            if ( @candidates || @reservelist ) {
                foreach ( @candidates, @reservelist ) {
                    next ENTRY
                      if $sha1 eq $sha1Machine->addfile($_)->digest;
                }
            }
            $returnValue->{$name} = $what;
        }

        $returnValue;

    };

}

1;
