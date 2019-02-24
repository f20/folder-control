package FileMgt106::FolderTidy;

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

use warnings;
use strict;
use Encode qw(decode_utf8 encode_utf8);
use File::Spec::Functions qw(catfile catdir);
use POSIX ();

use constant {
    STAT_NLINK => 3,
    STAT_MTIME => 9,
};

sub deepClean {
    my $count = 0;
    foreach my $folder (@_) {
        my @list;
        my $dh;
        opendir $dh, $folder or next;
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
        closedir $dh;
        foreach my $file (@list) {
            if ( $file eq '.git' ) {
                ++$count;
                next;
            }
            my $fullPath = "$folder/$file";
            if ( $file =~ /^(?:\.|\:2e)(?:DS_Store$|_)/ ) {
                unlink $fullPath;
                next;
            }
            my $newPath;
            $newPath = "$folder/_$file" if $file =~ /^(\~\$|Z_|\.)/is;
            $newPath = "$folder/${file}_"
              if $file =~ /\.(?:app|aplibrary|download|lrcat|lrdata|tmp)$/is;
            if ($newPath) {
                if ( -e $newPath ) {
                    my ( $base, $extension ) =
                      ( $newPath =~ m#(.*)(\.[^ /]+)$#s );
                    ( $base, $extension ) = ( $newPath, '' )
                      unless defined $extension;
                    my $c = 2;
                    while ( -e ( $newPath = "$base~$c$extension" ) ) { ++$c; }
                }
                $fullPath = $newPath if rename $fullPath, $newPath;
            }
            my $nlinks = ( lstat $fullPath )[STAT_NLINK];
            ++$count
              unless -d _ and !deepClean($fullPath) and rmdir $fullPath
              or -l _ || -z _ and unlink $fullPath
              or -f _
              and $nlinks > 1
              and unlink $fullPath;
        }
    }
    $count;
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
            my $mtime = ( lstat $prefix . $p2 )[STAT_MTIME];
            if ( -d _ ) {
                $mtime = $datemarker->($p2);
            }
            elsif ( !-f _ || !-s _ ) {
                $mtime = 0;
            }
            $maxt = $mtime if $mtime > $maxt;
        }
        my $np = $path;
        if ( $maxt && length $path ) {
            my $date = POSIX::strftime( '%Y-%m-%d', localtime($maxt) );
            $np =~
              s#(?:[XY]_|_| |[0-9]{4}-[0-9]{2}-[0-9]{2})*([^/]*)$#Y_$date $1#s;
            $np =~ s/ +$//s;
            if ( $np ne $path ) {
                $np .= '_' while -e $prefix . $np;
                rename $prefix . $path, $prefix . $np;
            }
        }
        utime time, $maxt, $prefix . $np
          if $maxt;    # only works if we are root or the folder's owner
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
            my $mtime = ( lstat $p2 )[STAT_MTIME];
            if ( -d _ ) {
                $mtime = $restamper->($p2);
            }
            elsif ( !-f _ || !-s _ ) {
                $mtime = 0;
            }
            $maxt = $mtime if $mtime > $maxt;
        }
        utime time, $maxt, $path
          if $maxt;    # only works if we are root or the folder's owner
        $maxt;
    };
    $restamper->( defined $_[0] && length $_[0] ? "$_[0]" : '.' );
}

sub automaticNumbering {
    my ( $path, $contents ) = @_;
    return unless ref $contents eq 'HASH';
    my $numberPadding = 0;
    my $highestNumber = 0;
    my @statusByNumber;
    my @toBeNumbered = grep {
        if (/^( *)([0-9]+)\. /s) {
            if ( length $1 ) {
                my $l = length( $1 . $2 );
                $numberPadding = $l if $l > $numberPadding;
            }
            $highestNumber = $2 if $2 > $highestNumber;
            ++$statusByNumber[$2];
            0;
        }
        else {
            1;
        }
    } keys %$contents;
    foreach my $number (
        grep { defined $statusByNumber[$_] && $statusByNumber[$_] > 1; }
        1 .. $highestNumber )
    {
        push @toBeNumbered, grep { /^ *$number\. /s; } keys %$contents;
    }
    push @toBeNumbered, grep { /^[ 0-9]{1,@{[$numberPadding-1]}}\. /s; }
      keys %$contents
      if $numberPadding;
    return unless @toBeNumbered;
    restampFolder($path);
    foreach (
        sort { $a->[1] <=> $b->[1]; } map {
            my $p = catdir( $path, $_ );
            my @s = stat $p;
            @s ? [ $_, $s[STAT_MTIME], $p, -d _ ] : ();
        } @toBeNumbered
      )
    {
        my $name = $_->[0];
        my $number;
        if ( $name =~ s/^ *([0-9]+)\. //s && $statusByNumber[$1] ) {
            $number = $1;
            undef $statusByNumber[$1];
        }
        $number ||= ++$highestNumber;
        $number = " $number" while length($number) < $numberPadding;
        $name = "$number. $name";
        if ( $_->[3] ) {
            rename $_->[2], catdir( $path, $name )
              unless $_->[0] eq $name;
        }
        else {
            $name =~ s/\.[0-9a-z]+$//si;
            my $newFolder = catdir( $path, $name );
            mkdir $newFolder;
            rename $_->[2], catfile( $newFolder, $_->[0] );
        }
    }
}

sub categoriseByDay {
    my ($path) = @_;
    my $maxt = 0;
    my $dh;
    opendir $dh, $path or return;
    my @list = map { decode_utf8 $_; } grep { !/^\./s } readdir $dh;
    foreach (@list) {
        my $p2    = "$path/$_";
        my $mtime = ( lstat $p2 )[STAT_MTIME];
        next unless -f _;
        my $date = POSIX::strftime( '%Y-%m-%d', localtime($mtime) );
        mkdir "$path/$date";
        my $p3 = "$path/$date/$_";
        $p3 .= '_' while -e $p3;
        rename $p2, $p3;
    }
}

1;
