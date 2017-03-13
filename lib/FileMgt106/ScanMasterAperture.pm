package FileMgt106::ScanMasterAperture;

=head Copyright licence and disclaimer

Copyright 2011-2017 Franck Latrémolière.

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
use utf8;
use Cwd;
use Sys::Hostname qw(hostname);
use Encode qw(decode_utf8 encode_utf8);
use FileMgt106::Scanner;
use FileMgt106::FileSystem;

use constant {
    LIB_DIR   => 0,
    LIB_MTIME => 1,
    LIB_JBZ   => 2,
};

sub new {
    my $class = shift;
    bless [@_], $class;
}

sub findOrMakeApertureLibraries {
    my $class   = shift;
    my $hints   = shift;
    my @liblist = map { /\.aplibrary(\/*|\.jbz)$/s ? decode_utf8($_) : (); } @_;
    foreach my $lib ( grep { s/\.jbz$//; } @liblist ) {
        my @stat = stat "$lib.jbz";
        unless (@stat) {
            warn "Ignored: $lib.jbz";
            next;
        }
        eval {
            unless ( -d $lib ) {
                mkdir $lib or die "Failed to mkdir $lib";
            }
            require FileMgt106::LoadSave;
            my $target = FileMgt106::LoadSave::loadNormalisedScalar("$lib.jbz");
            delete $target->{$_} foreach grep { /\//; } keys %$target;
            $hints->beginInteractive;
            FileMgt106::Scanner->new( $lib, $hints,
                $hints->statFromGid( $stat[STAT_GID] ) )->scan( 0, $target );
            $hints->commit;
            utime time, $stat[STAT_MTIME], $lib;
        };
        warn "Failed to rebuild $lib: $@" if $@;
    }
    unless (@liblist) {
        my $path;
        my %paths;
        warn 'Looking for Aperture libraries';
        $hints->beginInteractive;
        my $q =
          $hints->{dbHandle}
          ->prepare('select name, parid from locations where locid=?');
        $path = sub {
            my ($locid) = @_;
            return $paths{$locid} if exists $paths{$locid};
            $q->execute($locid);
            my ( $name, $parid ) = $q->fetchrow_array;
            return $paths{$locid} = undef unless defined $parid;
            return $paths{$locid} = $name unless $parid;
            my $p = $path->($parid);
            return $paths{$locid} = undef unless defined $p;
            $paths{$locid} = "$p/$name";
        };
        @liblist =
          map { defined $_ ? decode_utf8($_) : (); }
          map { $path->( $_->[0] ); } @{
            $hints->{dbHandle}->selectall_arrayref(
                'select locid from locations where name like "%.aplibrary"')
          };
        $q->finish;
        $hints->commit;
    }
    map {
        my @t = stat "$_/Database/apdb";
        @t ? $class->new( $_, $t[STAT_MTIME] ) : ();
    } @liblist;
}

sub setPathsCheckUpToDate {
    my ( $lib, $jbzDir ) = @_;
    chdir $lib->[LIB_DIR] or return;
    local $_ = $lib->[LIB_DIR] = decode_utf8 getcwd();
    {
        my $location;
        if (s#/Volumes/(.*)/##gs) {
            $location = $1;
        }
        elsif (s#(.*)/##gs) {
            $location = $1;
            local $_ = hostname();
            s/\..*//;
            $location = $_ . $location;
        }
        if ($location) {
            $location =~ tr#/#.#;
            s/\.aplibrary$/ in $location/s;
        }
    }
    if ( length( encode_utf8 $_) > 200 ) {
        require Digest::SHA;
        $_ = substr( $_, 0, 150 ) . ' ' . Digest::SHA::sha1_hex($_);
    }
    my @stat = stat( $lib->[LIB_JBZ] = "$jbzDir/$_.aplibrary.jbz" );
    @stat and $stat[STAT_MTIME] > $lib->[LIB_MTIME];
}

sub updateJbz {
    my ( $lib, $hints, $jbzDir ) = @_;
    return if $lib->setPathsCheckUpToDate($jbzDir);
    require FileMgt106::ScannerAperture;
    my $jbz = FileMgt106::ScannerAperture->scan( $lib->[LIB_DIR], $hints );
    require FileMgt106::LoadSave;
    FileMgt106::LoadSave::saveJbzPretty( $lib->[LIB_JBZ] . $$, $jbz );
    rename $lib->[LIB_JBZ] . $$, $lib->[LIB_JBZ];
}

1;
