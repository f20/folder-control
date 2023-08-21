package FileMgt106::FilterFactory::ByType;

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

sub explodeByExtension {
    my ($what) = @_;
    my %newHash;
    while ( my ( $key, $val ) = each %$what ) {
        if ( ref $val eq 'HASH' ) {
            my ($exploded) = explodeByExtension($val);
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
            $newHash{$ext}{$key} = $val;
        }
    }
    \%newHash;
}

sub explodeByStorageCategory {
    my ($what) = @_;
    my %newHash;
    while ( my ( $key, $val ) = each %$what ) {
        if ( ref $val eq 'HASH' ) {
            my ($exploded) = explodeByStorageCategory($val);
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
            $newHash{
                  $key =~ /\.nef$/si                      ? 'compressed-nikon'
                : $key =~ /\.arw$/si                      ? 'compressible-sony'
                : $key =~ /\.(?:m4a|mp3|aac)$/si          ? 'compressed-audio'
                : $key =~ /\.(?:m4v|mp4|mov|mkv|webm|avi)$/si ? 'compressed-video'
                : $key =~ /\.(?:heic|jpe?g|gif|png)$/si   ? 'compressed-images'
                : $key =~ /\.(?:[a-z][bx]z|bz2|gz|xz|ipa|ipsw|m?pkg)$/si
                ? 'compressed-other'
                : $key =~ /\.(?:tiff?|psd|psb)$/si ? 'compressible-images'
                : 'compressible-other' # NB: PDFs and Office files are often compressible
            }{$key} = $val;
        }
    }
    \%newHash;
}

sub explodeByType {

    my ($what) = @_;
    my %newHash;

    while ( my ( $key, $val ) = each %$what ) {

        if ( ref $val eq 'HASH' ) {
            my ($exploded) = explodeByType($val);
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

            my ( $base, $ext ) = ( $key =~ m#(.*)(\.\S*)$#s );
            ( $base, $ext ) = ( $key, '' )
              unless defined $ext;
            $ext = lc $ext;

            my $cat = $ext eq '.' ? 'Email' : 'Other';
            $ext =~ s/^\.+//s;

            $cat = 'Android'
              if $ext eq 'apk';
            $cat = 'Aperture'
              if $ext =~ /^ap[a-jl-oq-z][a-z0-9]*$/;
            $cat = 'Audio'
              if $ext eq 'wav'
              || $ext eq 'mp3'
              || $ext eq 'm4a'
              || $ext eq 'flac'
              || $ext eq 'aa'
              || $ext eq 'aax';
            $cat = 'Config'
              if $ext eq 'conf'
              || $ext eq 'ini'
              || $ext eq 'plist'
              || $ext eq 'xml';
            $cat = 'Document'
              if $ext =~ /^doc/s
              || $ext eq 'pages'
              || $ext eq 'rtf'
              || $ext eq 'odt';
            $cat = 'Email' if $ext eq 'eml';
            $cat = 'iOS'
              if $ext eq 'ipa' || $ext eq 'ipsw';
            $cat = 'Image_jpg'
              if $ext eq 'jpg' || $ext eq 'jpeg';
            $cat = 'Image_tiff'
              if $ext eq 'tif'
              || $ext eq 'tiff'
              || $ext eq 'psd'
              || $ext eq 'psb';
            $cat = "Image_$ext"
              if $ext eq 'arw'
              || $ext eq 'dng'
              || $ext eq 'gif'
              || $ext eq 'heic'
              || $ext eq 'ico'
              || $ext eq 'nef'
              || $ext eq 'png'
              || $ext eq 'svg';
            $cat = 'JSON'
              if $ext eq 'jbz' || $ext eq 'json';
            $cat = 'Package'
              if $ext eq 'bz2'
              || $ext eq 'exe'
              || $ext eq 'gz'
              || $ext eq 'pkg'
              || $ext eq 'rar'
              || $ext eq 'tar'
              || $ext eq 'tbz'
              || $ext eq 'tgz'
              || $ext eq 'txz'
              || $ext eq 'xz'
              || $ext eq 'zip';
            $cat = 'PDF'
              if $ext eq 'pdf';
            $cat = 'Perl'
              if $ext eq 'pl' || $ext eq 'pm';
            $cat = 'Presentation'
              if $ext eq 'ppt' || $ext eq 'pptx' || $ext eq 'key';
            $cat = 'Python'
              if $ext eq 'py' || $ext eq 'pyc';
            $cat = 'Spreadsheet'
              if $ext =~ /^xl/s
              || $ext eq 'csv'
              || $ext eq 'numbers'
              || $ext eq 'ods';
            $cat = 'Text'
              if $ext eq 'txt';
            $cat = 'Video'
              if $ext eq 'mov'
              || $ext eq 'mp4'
              || $ext eq 'm4v'
              || $ext eq 'mkv'
              || $ext eq 'webm'
              || $ext eq 'avi';
            $cat = 'Volume'
              if $ext eq 'dmg'
              || $ext eq 'img'
              || $ext eq 'iso'
              || $ext eq 'sparseimage'
              || $ext eq 'vdi';
            $cat = 'Web'
              if $ext eq 'htm'
              || $ext eq 'html'
              || $ext eq 'css'
              || $ext eq 'js'
              || $ext eq 'php';

            $newHash{$cat}{$key} = $val;

        }
    }

    \%newHash;

}

1;
