package FileMgt106::Extraction::Spreadsheets;

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

sub _escapeCsv {
    ( local $_ ) = @_;
    s/"/""/g;
    qq%"$_"%;
}

sub _formatCsv {
    ( local $_ ) = @_;
    return "$1-$2-$3 $4"
      if /^([0-9]+)[-:]([0-9]+)[-:]([0-9]+)[ T]([0-9]+:[0-9]+:[0-9]+)$/s;
    $_;
}

sub makeCsvWriter {
    my ($dumpFile) = @_;
    $dumpFile ||= '';
    $dumpFile =~ s/[^%_\.a-zA-Z0-9]+/_/gs;
    my $c;
    if ($dumpFile) {
        open $c, '>', "$dumpFile.$$";
    }
    else {
        $c = \*STDOUT;
    }
    binmode $c, ':utf8';
    sub {
        if (@_) {
            print {$c} join( ',',
                map { /[" ]/ ? _escapeCsv($_)        : $_; }
                map { ref $_ ? _formatCsv( $_->[0] ) : $_; } @_ )
              . "\n";
        }
        else {
            close $c;
            rename "$dumpFile.$$", "$dumpFile.csv" if $dumpFile;
        }
    };
}

sub makeSpreadsheetWriter {
    my ( $format, $fileName ) = @_;
    my $module;
    if ( $format =~ /^xls$/s ) {
        if ( eval { require Spreadsheet::WriteExcel; } ) {
            $fileName .= '.xls';
            $module = 'Spreadsheet::WriteExcel';
        }
        else {
            warn "Could not load Spreadsheet::WriteExcel: $@";
        }
    }
    if ( !$module ) {
        if ( eval { require Excel::Writer::XLSX; } ) {
            $fileName .= '.xlsx';
            $module = 'Excel::Writer::XLSX';
        }
        else {
            warn "Could not load Excel::Writer::XLSX: $@";
        }
    }
    if ( !$module && eval { require Spreadsheet::WriteExcel; } ) {
        $fileName .= '.xls';
        $module = 'Spreadsheet::WriteExcel';
    }
    if ($module) {
        warn "Using $module";
        my ( $wb, $ws, $lastCol, $dateFormat, $sizeFormat );
        my $wsWrite = sub {
            ( my $ws, my $row, my $col, local $_ ) = @_;
            if ( ref $_ ) {
                ( local $_ ) = @$_;
                if (
/^([0-9]+)[-:]([0-9]+)[-:]([0-9]+)[ T]([0-9]+:[0-9]+:[0-9]+)$/s
                  )
                {
                    $ws->write_date_time(
                        $row, $col,
                        "$1-$2-$3" . 'T' . $4,
                        $dateFormat ||= $wb->add_format(
                            num_format => 'ddd d mmm yyyy HH:MM:SS'
                        )
                    );
                }
                elsif (/^[0-9]+$/s) {
                    $ws->write(
                        $row, $col, $_,
                        $sizeFormat ||= $wb->add_format(
                            num_format => '#,##0'
                        )
                    );
                }
                else {
                    $ws->write( $row, $col, $_ );
                }
            }
            else {
                $ws->write_string( $row, $col, $_ );
            }
        };

        my $row = -1;
        return sub {
            if (@_) {
                unless ($wb) {
                    $wb = $module->new( $fileName . $$ );
                    $ws = $wb->add_worksheet('Extracted');
                    $ws->set_paper(9);
                    $ws->fit_to_pages( 1, 0 );
                    $ws->hide_gridlines(2);
                }
                ++$row;
                unless ( defined $lastCol ) {
                    $lastCol = $#_;
                    my @colWidth = qw(8 24 12 8 48 48),
                      map { /date|time/i ? 48 : 12; } @_[ 6 .. $lastCol ];
                    $ws->set_column( $_, $_, $colWidth[$_] )
                      foreach 0 .. $lastCol;
                }
                $wsWrite->( $ws, $row, $_, $_[$_] ) foreach 0 .. $#_;
            }
            else {
                $ws->autofilter( 0, 0, $row, $lastCol );
                $wb->close;
                undef $wb;
                rename $fileName . $$, $fileName;
            }
        };
    }
    warn 'Using CSV';
    makeCsvWriter($fileName);
}

use strict;
use warnings;
use utf8;
use Digest::SHA ();
use Encode qw(decode_utf8);
use File::Spec::Functions qw(catdir catfile);
use FileMgt106::Database;
use FileMgt106::FileSystem;

1;
