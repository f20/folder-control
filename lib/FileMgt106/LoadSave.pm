package FileMgt106::LoadSave;

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
use Encode qw(decode_utf8);
use Unicode::Normalize;

my $jsonMachine;

sub jsonMachineMaker {
    return $jsonMachine if $jsonMachine;
    foreach (qw(JSON JSON::PP)) {
        return $jsonMachine = $_->new->canonical->pretty
          if eval "require $_";
    }
    die 'No JSON module';
}

my $normaliser = -e '/System/Library' ? \&NFD : \&NFC;

sub setNormalisation {
    return unless local $_ = "@_";
    $normaliser =
        /ascii/i ? sub { local $_ = NFKD( $_[0] ); s/[^ -~]/_/g;    $_; }
      : /win/i   ? sub { local $_ = NFC( $_[0] );  tr/[]?\/\\|:/_/; $_; }
      : /nfkd/i  ? \&NFKD
      : /nfkc/i  ? \&NFKC
      : /nfd/i   ? \&NFD
      : /nfc/i   ? \&NFC
      :            $normaliser;
}

sub normaliseFileNames {
    my ($dir) = @_;
    opendir DIR, $dir or die "opendir: $! in " . `pwd`;
    my @list = map { decode_utf8 $_; } readdir DIR;
    closedir DIR;
    foreach (@list) {
        next if /^\.(?:\.?$|_)/ || $_ eq '.DS_Store' || $_ eq '.git';
        my $norm = $normaliser->($_);
        $norm =~ s/^(\~\$|Z_|\.)/_$1/is;
        $norm =~ s/\.(download|tmp|aplibrary)$/.${1}_/is;
        my $path = "$dir/$_";
        if ( $norm ne $_ ) {
            my $d3 = "$dir/$norm";
            if ( -e $d3 ) {
                my ( $base, $ext ) = ( $d3 =~ m#(.*)(\.[^ /]+)$#s );
                ( $base, $ext ) = ( $d3, '' ) unless defined $ext;
                my $c = 2;
                while ( -e ( $d3 = "$base~$c$ext" ) ) { ++$c; }
            }
            if ( rename $path, $d3 ) {
                $path = $d3;
            }
            else {
                warn "rename $path -> $d3 failed: $! in " . `pwd`;
            }
        }
        lstat $path;
        normaliseFileNames($path) if -d _;
    }
}

sub normaliseHash {
    my ($hr) = @_;
    return $hr unless ref $hr eq 'HASH';
    my @original   = sort keys %$hr;
    my @normalised = map { $normaliser->($_); } @original;
    my @unique     = map { lc($_); } @normalised;
    my %map;
    foreach ( 0 .. $#original ) {
        my $u = $unique[$_];
        if ( !defined $map{$u} ) {
            $map{$u} = 0;
        }
        elsif ( !$map{$u} ) {
            $map{$u} = 1;
        }
    }
    foreach ( 0 .. $#original ) {
        my $o = $original[$_];
        my $n = $normalised[$_];
        my $u = $unique[$_];
        if ( $map{$u} ) {
            my $c;
            do {
                $c = $u . ' ~' . $map{$u}++;
            } while defined $map{$c};
            $map{$c} = 0;
            $hr->{$c}{$n} = normaliseHash( delete $hr->{$o} );
        }
        elsif ( $n ne $o ) {
            $hr->{$n} = normaliseHash( delete $hr->{$o} );
        }
        else {
            normaliseHash( $hr->{$o} );
        }
    }
    $hr;
}

sub loadNormalisedScalar {
    ( local $_ ) = @_;
    my $obj;
    eval {
        my $fh;
        if (/\.(?:jbz|bz2)$/i) {
            s/'/'"'"'/g;
            open $fh, "bzcat '$_'|";
        }
        else {
            open $fh, '<', $_;
        }
        local undef $/;
        binmode $fh;
        $obj = jsonMachineMaker()->decode(<$fh>);
    };
    $obj ? normaliseHash($obj) : undef;
}

sub saveBzOctets {
    my ( $file, $blob ) = @_;
    $file =~ s/'/'"'"'/g;
    open my $fh, qq%|bzip2>'$file'% or goto FAIL;
    binmode $fh or goto FAIL;
    print {$fh} $blob or goto FAIL;
    return 1;
  FAIL: warn $!;
    return;
}

sub saveJbz {
    saveBzOctets( $_[0], jsonMachineMaker()->encode( $_[1] ) );
}

sub parseText {
    my ($file) = @_;
    open my $fh, '<', $file;
    binmode $fh, ':utf8';
    local $/ = "\n";
    local $_;
    my $obj = {};
    while (<$fh>) {
        next
          unless my ( $path, $sha1 ) = /([^=:" ][^=:"\t]*).*([a-fA-F0-9]{40})/;
        my $o = $obj;
        my @pathEl = split /\/+/, $path;
        while ( @pathEl > 1 ) {
            $o = $o->{ shift @pathEl } ||= {};
        }
        $o->{ $pathEl[0] } = $sha1;
    }
    $obj;
}

sub makeInfillFilter {
    my %done;
    my $filter;
    $filter = sub {
        my ($hash) = @_;
        my %newHash;
        foreach ( sort { length $a <=> length $b } keys %$hash ) {
            my $what = $hash->{$_};
            if ( ref $what eq 'HASH' ) {
                $what = $filter->($what);
                $newHash{$_} = $what if $what;
            }
            elsif ( $what && !$done{$what} ) {
                $done{$what} = 1;
                s/\s+/ /gs;
                s/^ //;
                s/ \././g;
                s/ $//;
                $newHash{ $_ || ( '__' . rand() ) } = $what;
            }
        }
        keys %newHash ? \%newHash : undef;
    };
}

1;
