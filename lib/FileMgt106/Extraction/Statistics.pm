package FileMgt106::Extraction::Statistics;

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

use strict;
use warnings;
use FileMgt106::Database;

sub _prettyNumber {
    my ( $after, $before, $spaces ) = @_;
    $after ||= 0;
    $after -= $before if $before;
    do { } while $after =~ s/([0-9])([0-9]{3})(?:,|$)/$1,$2/s;
    $spaces -= length $after;
    ( $spaces > 0 ? ' ' x $spaces : '' ) . $after;
}

sub makeStatisticsExtractor {
    my ($hintsFile) = @_;
    binmode STDOUT, ':utf8';
    my $hints = FileMgt106::Database->new( $hintsFile, 1 );
    my $query =
      $hints->{dbHandle}->prepare('select size from locations where sha1=?');
    my (
        %seen,   $found, $missing, $dups, $bytes,
        $bytes2, %found, %missing, %dups, %bytes
    );
    my $processor;
    $processor = sub {
        my ($cat) = @_;
        while ( my ( $k, $v ) = each %$cat ) {
            if ( 'HASH' eq ref $v ) {
                $processor->($v);
                next;
            }
            if ( $v =~ /([a-fA-F0-9]{40})/ ) {
                my $sha1hex = lc $1;
                my ($ext) = $k =~ /\.([a-zA-Z0-9_]+)$/s;
                if ( exists $seen{$sha1hex} ) {
                    ++$dups;
                    ++$dups{$ext} if defined $ext;
                    $bytes2 += $seen{$sha1hex} if defined $seen{$sha1hex};
                }
                else {
                    $query->execute( pack( 'H*', $sha1hex ) );
                    my ($b) = $query->fetchrow_array;
                    $query->finish;
                    $seen{$sha1hex} = $b;
                    if ( defined $b ) {
                        ++$found;
                        ++$found{$ext} if defined $ext;
                        $bytes  += $b;
                        $bytes2 += $b;
                        $bytes{$ext} += $b if defined $ext;
                    }
                    else {
                        ++$missing;
                        ++$missing{$ext} if defined $ext;
                    }
                }
            }
        }
    };
    sub {
        my ( $scalar, $name ) = @_;
        unless ( defined $name ) {
            print _prettyNumber( $found, undef, 10 )
              . ' found, '
              . _prettyNumber( $bytes, undef, 15 )
              . ' bytes, '
              . _prettyNumber( $missing, undef, 7 )
              . ' missing, '
              . _prettyNumber( $dups, undef, 7 )
              . ' duplicated, '
              . _prettyNumber( $bytes2, undef, 11 )
              . " bytes including duplication.\n";
            return;
        }
        my $startFound   = $found;
        my $startMissing = $missing;
        my $startDups    = $dups;
        my $startBytes   = $bytes;
        $processor->($scalar);
        print _prettyNumber( $found, $startFound, 10 )
          . ' found, '
          . _prettyNumber( $bytes, $startBytes, 15 )
          . ' bytes, '
          . _prettyNumber( $missing, $startMissing, 7 )
          . ' missing, '
          . _prettyNumber( $dups, $startDups, 7 )
          . ' duplicated, '
          . "$name\n";
        return;
    };
}

1;
