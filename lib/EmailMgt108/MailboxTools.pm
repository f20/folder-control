package EmailMgt108::MailboxTools;

# Copyright 2020 Franck Latrémolière, Reckon LLP.
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
use Encode qw(decode_utf8);
use File::Spec::Functions qw(catdir catfile);
use FileMgt106::Database;
use FileMgt106::FileSystem qw(STAT_DEV STAT_INO STAT_MODE STAT_UID);
use FileMgt106::Folders::Builder;
use Time::Piece;

sub makeMailboxConsolidator {

    my ( $hintsFile, $archiveFlag ) = @_;
    my $hintsBuilder =
      FileMgt106::Folders::Builder::makeHintsBuilder($hintsFile);

    sub {

        my ( $whatYouWant, $whereYouWantIt, $devNo ) = @_;
        return unless ref $whatYouWant eq 'HASH';

        my %hashSet;
        my $scanner;
        $scanner = sub {
            my ($cat) = @_;
            while ( my ( $k, $v ) = each %$cat ) {
                if    ( ref $v eq 'HASH' ) { $scanner->($v); }
                elsif ( $k =~ s/\.$//s )   { undef $hashSet{$v}; }
            }
        };
        $scanner->($whatYouWant);
        my $numFiles    = keys %hashSet;
        my $digits      = length( $numFiles - 1 );
        my $id1         = 1 * 10**$digits;
        my %cat1        = map { $id1++ . '.' => $_; } keys %hashSet;
        my $returnValue = $hintsBuilder->( \%cat1, $whereYouWantIt, $devNo );

        require EmailMgt108::EmailParser if $archiveFlag;
        {
            my %sortKey;
            foreach my $file ( keys %cat1 ) {
                $file = catfile( $whereYouWantIt, $file )
                  if defined $whereYouWantIt;
                my @stat = stat $file or next;
                my %stamp;
                open my $fh, '<', $file;
                local $/ = "\n";
                while (<$fh>) {
                    last if /^\s*$/s;
                    $stamp{ lc $1 } =
                      Time::Piece->strptime( $2, '%a, %d %b %Y %H:%M:%S %z' )
                      ->epoch
                      if
/^(\S*date\S*):\s+(\S+,\s+[0-9]+\s+\S+\s+[0-9]{4}\s+[0-9]{2}:[0-9]{2}:[0-9]{2}\s+[+-][0-9]{4})/i;
                }
                my $contentStamp = $stamp{'delivered-date'};
                ($contentStamp) = sort { $b <=> $a; } values %stamp
                  unless defined $contentStamp;
                if ( defined $contentStamp && $stat[9] != $contentStamp ) {
                    utime time, $contentStamp, $file;
                    $sortKey{$file} = $contentStamp;
                }
                else {
                    $sortKey{$file} = $stat[9];
                }
            }
            my $id2 = 2 * 10**$digits;
            foreach (
                sort { $sortKey{$a} <=> $sortKey{$b}; }
                keys %sortKey
              )
            {
                my $target = $id2++ . '.';
                $target = catfile( $whereYouWantIt, $target )
                  if defined $whereYouWantIt;
                rename $_, $target;
                EmailMgt108::EmailParser::parseMessage($target)
                  if $archiveFlag;
            }
        }

        $returnValue;

    };

}

1;
