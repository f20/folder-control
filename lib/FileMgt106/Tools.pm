package FileMgt106::Tools;

=head Copyright licence and disclaimer

Copyright 2011-2015 Franck Latrémolière, Reckon LLP.

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
use JSON;
use Encode qw(decode_utf8 encode_utf8);
use Unicode::Normalize;
require POSIX;

my $normaliser = -e '/System/Library' ? \&NFD : \&NFC;

sub setNormalisation {
    return unless local $_ = "@_";
    $normaliser =
        /ascii/i ? sub { local $_ = NFKD( $_[0] ); s/[^ -~]/_/g; $_; }
      : /nfkd/i  ? \&NFKD
      : /nfkc/i  ? \&NFKC
      : /nfd/i   ? \&NFD
      : /nfc/i   ? \&NFC
      :            $normaliser;
}

sub normaliseHash {
    my ( $hr, $keyFilter ) = @_;
    return $hr unless ref $hr eq 'HASH';
    if ($keyFilter) {
        delete $hr->{$_} foreach grep { !$keyFilter->($_); } keys %$hr;
    }
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
                $c = $u . '+++' . $map{$u}++;
            } while defined $map{$c};
            $map{$c} = 0;
            $hr->{$c}{$n} = normaliseHash( delete $hr->{$o}, $keyFilter );
        }
        elsif ( $n ne $o ) {
            $hr->{$n} = normaliseHash( delete $hr->{$o}, $keyFilter );
        }
        else {
            normaliseHash( $hr->{$o}, $keyFilter );
        }
    }
    $hr;
}

sub parseText {
    my ($file) = @_;
    open my $fh, '<', $file;
    binmode $fh, ':utf8';
    local $/ = "\n";
    local $_;
    my $obj = {};
    while (<$fh>) {
        next unless my ( $path, $sha1 ) = /([^=:" ][^=:"]*).*([a-fA-F0-9]{40})/;
        my $o = $obj;
        my @pathEl = split /\/+/, $path;
        while ( @pathEl > 1 ) {
            $o = $o->{ shift @pathEl } ||= {};
        }
        $o->{ $pathEl[0] } = $sha1;
    }
    $obj;
}

sub loadJbz {
    ( local $_, my $keyFilter ) = @_;
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
        $obj = decode_json(<$fh>);
    };
    $obj ? normaliseHash( $obj, $keyFilter ) : undef;
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
    saveBzOctets( $_[0], JSON->new->canonical(1)->utf8->encode( $_[1] ) );
}

sub saveJbzPretty {
    saveBzOctets( $_[0],
        JSON->new->canonical(1)->utf8->pretty->encode( $_[1] ) );
}

sub explodeByType {
    my ($what) = @_;
    my %newHash;
    while ( my ( $key, $val ) = each %$what ) {
        if ( ref $val eq 'HASH' ) {
            my $exploded = explodeByType($val);
            while ( my ( $ext, $con ) = each %$exploded ) {
                if ( $key eq $ext && ref $con eq 'HASH' ) {
                    foreach ( keys %$con ) {
                        my $new = $_;
                        $new .= '_' while exists $newHash{$key}{$new};
                        $newHash{$key}{$new} = $con->{$_};
                    }
                }
                else {
                    $newHash{$ext}{$key} = $con;
                }
            }
        }
        else {
            my ( $base, $ext ) = ( $key =~ m#(.*)(\.\S+)$#s );
            ( $base, $ext ) = ( $key, '' )
              unless defined $ext;
            $ext = lc $ext;
            $ext =~ s/^\.+//s;
            my $cat = 'Other';
            $cat = 'PDF'  if $ext eq 'pdf';
            $cat = 'Perl' if $ext eq 'pl' || $ext eq 'pm';
            $cat = 'Text' if $ext eq 'txt';
            $cat = 'Web'
              if $ext eq 'htm'
              || $ext eq 'html'
              || $ext eq 'css'
              || $ext eq 'php';
            $cat = 'Archive'
              if $ext eq 'zip'
              || $ext eq 'tar'
              || $ext eq 'gz'
              || $ext eq 'rar'
              || $ext eq 'bz2'
              || $ext eq 'tgz'
              || $ext eq 'tbz';
            $cat = 'Image'
              if $ext eq 'jpg'
              || $ext eq 'png'
              || $ext eq 'gif'
              || $ext eq 'jpeg';
            $cat = 'Sound'
              if $ext eq 'wav'
              || $ext eq 'mp3'
              || $ext eq 'm4a';
            $cat = 'Video'
              if $ext eq 'mov'
              || $ext eq 'mp4'
              || $ext eq 'm4v';
            $cat = 'Spreadsheet'
              if $ext eq 'csv' || $ext eq 'raw' || $ext =~ /^xl/s;
            $cat = 'Presentation' if $ext eq 'ppt' || $ext eq 'pptx';
            $cat = 'Document'
              if $ext eq 'doc' || $ext eq 'docx' || $ext eq 'rtf';
            $newHash{$cat}{$key} = $val;
        }
    }
    \%newHash;
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

sub makeHintsFilter {
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

sub _pretty {
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

sub flattenCwd {
    require Digest::SHA;
    my $flatten;
    $flatten = sub {
        my $r = $_[0];
        my $r_ = $r eq '.' ? '' : " in$r";
        $r_ =~ tr#/.#_ #;
        my $d;
        opendir $d, $r;
        foreach (
            grep {
                     !/^\.{1,2}$/s
                  && $_ ne '.DS_Store'
                  && !( $r eq '.' && /^Y_.* folder$/s );
            } readdir $d
          )
        {
            my $p = "$r/$_";
            my ( $inode, $lmod ) = ( stat $p )[ 1, 9 ];
            if ( -d _ ) {
                $flatten->($p);
                next;
            }
            my $e = '';
            $e = $1 if s/(\.[a-zA-Z][a-zA-Z0-9_+-]+)$//s;
            s/\s*\([0-9]{6,}\)//sg;
            my $fol = 'Y_' . ( lc $e ) . ' folder';
            mkdir $fol unless -d $fol;
            my $date = POSIX::strftime( '%Y-%m-%d', localtime $lmod );
            my $name = ( /^$date/ ? '' : "$date " ) . $_ . $r_;
            $name =
              substr( $name, 0, 70 ) . ' #x' . Digest::SHA::sha1_hex($name)
              if length( encode_utf8 $name) > 120;
            link $p, join '', $fol, '/', $name, $e
              or warn $p;
        }
    };
    $flatten->('.');
}

sub deepClean {
    my $count = 0;
  ITEM: foreach (@_) {
        my @list;
        {
            my $dh;
            opendir $dh, $_ or return;
            @list = sort {
                (
                      $b =~ /^(?:\~?\$|Z?_)/s ? "X $b"
                    : $b =~ /\.tmp$/si        ? "T $b"
                    : $b =~ /^Y_.* folder$/s  ? "B $b"
                    :                           "E $b"
                  ) cmp(
                    $a =~ /^(?:\~?\$|Z?_)/s  ? "X $a"
                    : $a =~ /\.tmp$/si       ? "T $a"
                    : $a =~ /^Y_.* folder$/s ? "B $a"
                    :                          "E $a"
                  )
            } grep { !/^\.\.?$/s } readdir $dh;
        }
        foreach my $file (@list) {
            if ( $file eq '.git' ) {
                ++$count;
                next;
            }
            my $d2 = "$_/$file";
            if ( $file =~ /^(?:\.|\:2e)(?:DS_Store$|Parent$|_)/ ) {
                unlink $d2;
                next;
            }
            if ( $file =~ s/^(\~\$|Z_|\.)/_$1/is ) {
                my $d3 = "$_/$file";
                if ( -e $d3 ) {
                    my ( $base, $extension ) = ( $d3 =~ m#(.*)(\.[^ /]+)$#s );
                    ( $base, $extension ) = ( $d3, '' )
                      unless defined $extension;
                    my $c = 2;
                    while ( -e ( $d3 = "$base~$c$extension" ) ) { ++$c; }
                }
                $d2 = $d3 if rename $d2, $d3;
            }
            if ( $file =~ s/\.(download|tmp|aplibrary)$/.${1}_/is ) {
                my $d3 = "$_/$file";
                if ( -e $d3 ) {
                    my ( $base, $extension ) = ( $d3 =~ m#(.*)(\.\S+)$#s );
                    ( $base, $extension ) = ( $d3, '' )
                      unless defined $extension;
                    my $c = 2;
                    while ( -e ( $d3 = "$base~$c$extension" ) ) { ++$c; }
                }
                $d2 = $d3 if rename $d2, $d3;
            }
            my $nlinks = ( lstat $d2 )[3];
            if ( undef && @list == 1 && -d _ ) {
                warn "$d2";
                my $dh;
                my $d3 = rand();
                opendir $dh, $d2;
                my @sub = grep { !/^\.\.?$/s } readdir $dh;
                closedir $dh;
                $d3 = rand() while grep { $d3 eq $_ } @sub;
                $d3 = "$_/$d3";

                if ( rename $d2, $d3 ) {

                    foreach my $file (@sub) {
                        rename "$d3/$file", "$_/$file";
                    }
                }
                ++$count if deepClean($_);
            }
            else {
                ++$count
                  unless -d _ and !deepClean($d2) and rmdir $d2
                  or -l _ || -z _ and unlink $d2
                  or -f _
                  and $nlinks > 1
                  and unlink $d2;
            }
        }
    }
    $count;
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

sub datemarkFolder {
    my $prefix = defined $_[0] && length $_[0] ? "$_[0]" : '.';
    my $datemarker;
    $datemarker = sub {
        my ($path) = @_;
        my $maxt = 0;
        my $dh;
        opendir $dh, $prefix . $path or return;
        my @list =
          map { decode_utf8 $_; } grep { !/^(?:|cyrus)\./s } readdir $dh;
        foreach (@list) {
            my $p2    = "$path/$_";
            my $mtime = ( lstat $prefix . $p2 )[9];
            if ( -d _ ) {
                $mtime = $datemarker->($p2);
            }
            elsif ( !-f _ ) {
                $mtime = 0;
            }
            $maxt = $mtime if $mtime > $maxt;
        }
        my $np = $path;
        if ( $maxt && length $path ) {
            my $date = POSIX::strftime( '%Y-%m-%d', localtime($maxt) );
            $np =~ s#(?:[XY]_)?(?:[()_ ]|$date|[0-9]{4}-[0-9]{2}-[0-9]{2})*([^/]*)$#Y_$date $1#s;
            $np =~ s/ +$//s;
            if ( $np ne $path ) {
                $np .= '_' while -e $prefix . $np;
                rename $prefix . $path, $prefix . $np;
            }
        }
        utime time, $maxt, $prefix . $np;    # only works if folder owner
        $maxt;
    };
    $datemarker->('');
}

sub restampFolder {
    my $restamper;
    $restamper = sub {
        my ($path) = @_;
        my $maxt = 0;
        my $dh;
        opendir $dh, $path or return;
        my @list =
          map { decode_utf8 $_; } grep { !/^(?:|cyrus)\./s } readdir $dh;
        foreach (@list) {
            my $p2    = "$path/$_";
            my $mtime = ( lstat $p2 )[9];
            if ( -d _ ) {
                $mtime = $restamper->($p2);
            }
            elsif ( !-f _ ) {
                $mtime = 0;
            }
            $maxt = $mtime if $mtime > $maxt;
        }
        utime time, $maxt, $path;    # only works if folder owner
        $maxt;
    };
    $restamper->( defined $_[0] && length $_[0] ? "$_[0]" : '.' );
}

sub categoriseByDay {
    my ($path) = @_;
    my $maxt = 0;
    my $dh;
    opendir $dh, $path or return;
    my @list = map { decode_utf8 $_; } grep { !/^\./s } readdir $dh;
    foreach (@list) {
        my $p2    = "$path/$_";
        my $mtime = ( lstat $p2 )[9];
        next unless -f _;
        my $date = POSIX::strftime( '%Y-%m-%d', localtime($mtime) );
        mkdir "$path/$date";
        my $p3 = "$path/$date/$_";
        $p3 .= '_' while -e $p3;
        rename $p2, $p3;
    }
}

1;
