package FileMgt106::Folders::FolderOrganise;

# Copyright 2011-2023 Franck Latrémolière and others.
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
use utf8;
use Encode qw(decode_utf8 encode_utf8);
use File::Spec::Functions qw(catfile catdir);
use FileMgt106::FileSystem qw(STAT_MTIME);
use POSIX ();

sub flattenCwd {
    require Digest::SHA;
    my $flatten;
    $flatten = sub {
        my $r  = $_[0];
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
            my $p    = "$r/$_";
            my $lmod = ( stat $p )[STAT_MTIME];
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
          if $maxt;    # only works if we are root or the folder's owner
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
          if $maxt;    # only works if we are root or the folder's owner
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
    my $forceNumbering;
    my @toBeNumbered = grep {
        if ( my ( $prefix, $number ) = /^([@# ]?)([ 0-9]+)\. /s ) {
            if ( length $prefix ) {
                my $len = length($number);
                ++$len                if $prefix eq ' ';
                $numberPadding = $len if $len > $numberPadding;
            }
            $highestNumber = $number if $number > $highestNumber;
            ++$statusByNumber[$number];
            $forceNumbering ||=
              [ $number, length($prefix) ? length($number) : 0, $_ ]
              if /force renumber/i && !/(?: done| \(done\))$/is;
            0;
        }
        else {
            !/^\./s;
        }
    } keys %$contents;

    if ($forceNumbering) {
        $highestNumber = $forceNumbering->[0];
        $numberPadding = $forceNumbering->[1];
        @toBeNumbered =
          grep { !/^\./s && $_ ne $forceNumbering->[2]; } keys %$contents;
        my $highestNumberLength = length( $highestNumber + @toBeNumbered );
        $numberPadding = $highestNumberLength
          if $numberPadding < $highestNumberLength;
        @statusByNumber = ();
        my $newName = $forceNumbering->[2];
        $newName =~ s/^[@# ]?[ 0-9]+//s;
        $newName =~ s/\s*$/ done/s unless $newName =~ /persistent/i;
        rename catdir( $path, $forceNumbering->[2] ),
          catdir(
            $path,
            (
                $numberPadding
                ? '#'
                  . '0' x ( $numberPadding - length( $forceNumbering->[0] ) )
                : ''
              )
              . $forceNumbering->[0]
              . $newName
          );
    }
    else {
        @toBeNumbered = grep { !/^\./s && !/^#[0-9]{$numberPadding}\. /s; }
          keys %$contents
          if $numberPadding;
        foreach my $number (
            grep { defined $statusByNumber[$_] && $statusByNumber[$_] > 1; }
            1 .. $highestNumber )
        {
            $number =
              '#' . ( '0' x ( $numberPadding - length($number) ) ) . $number
              if $numberPadding;
            push @toBeNumbered, grep { /^$number\. /s; } keys %$contents;
        }
    }

    return unless @toBeNumbered;
    restampFolder($path);
    foreach (
        sort { $a->[1] <=> $b->[1] || $a->[0] cmp $b->[0]; } map {
            my $p = catdir( $path, $_ );
            my @s = stat $p or return;     # give up if something has moved
            [ $_, $s[STAT_MTIME], $p, -d _ ];
        } @toBeNumbered
      )
    {
        my $name = $_->[0];
        my $number;
        if ( $name =~ s/^[@# ]*([0-9]+)\. +//s && $statusByNumber[$1] ) {
            $number = $1;
            undef $statusByNumber[$1];
        }
        $number ||= ++$highestNumber;
        $number = '#' . ( '0' x ( $numberPadding - length($number) ) ) . $number
          if $numberPadding;
        $name =~ s/^(?:Ω\s*|#|[A-Z]_)//;
        $name = "$number. $name";
        if ( $_->[3] ) {
            rename $_->[2], catdir( $path, $name )
              unless $_->[0] eq $name;
        }
        else {
            $name =~ s/\.[0-9a-z]+$//si;
            $name =~ s/[ _]+/ /g;
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
        my $mtime = ( stat $p2 )[STAT_MTIME];
        next unless -f _;
        my $date = POSIX::strftime( '%Y-%m-%d', localtime($mtime) );
        mkdir "$path/$date";
        my $p3 = "$path/$date/$_";
        $p3 .= '_' while -e $p3;
        rename $p2, $p3;
    }
}

1;
