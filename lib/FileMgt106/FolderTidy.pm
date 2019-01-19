package FileMgt106::FolderTidy;

# Copyright 2011-2017 Franck Latrémolière, Reckon LLP.
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
require POSIX;

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
            my $mtime = ( lstat $prefix . $p2 )[9];
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
            my $mtime = ( lstat $p2 )[9];
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
