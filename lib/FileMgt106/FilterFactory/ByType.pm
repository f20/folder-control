package FileMgt106::FilterFactory::ByType;

=head Copyright licence and disclaimer

Copyright 2011-2018 Franck Latrémolière, Reckon LLP.

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

            $cat = 'Aperture'
              if $ext =~ /^ap[a-oq-z]+$/;
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
            $cat = 'Database'
              if $ext eq 'db' || $ext eq 'apdb';
            $cat = 'Document'
              if $ext =~ /^doc/s
              || $ext eq 'pages'
              || $ext eq 'rtf'
              || $ext eq 'odt';
            $cat = 'iOS'
              if $ext eq 'ipa' || $ext eq 'ipsw';
            $cat = 'Image_jpg'
              if $ext eq 'jpg' || $ext eq 'jpeg';
            $cat = 'Image_tiff'
              if $ext eq 'tif' || $ext eq 'tiff';
            $cat = "Image_$ext"
              if $ext eq 'arw'
              || $ext eq 'dng'
              || $ext eq 'gif'
              || $ext eq 'heic'
              || $ext eq 'ico'
              || $ext eq 'nef'
              || $ext eq 'png'
              || $ext eq 'psd'
              || $ext eq 'svg'
              || $ext eq 'tiff';
            $cat = 'Image_tiff'
              if $ext eq 'tif';
            $cat = 'JBZ'
              if $ext eq 'jbz';
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
              || $ext eq 'avi';
            $cat = 'Volume'
              if $ext eq 'dmg'
              || $ext eq 'img'
              || $ext eq 'iso'
              || $ext eq 'sparseimage'
              || $ext eq 'vdi';
            $cat = 'Web'
              if $ext eq 'htm'
              || $ext eq 'js'
              || $ext eq 'html'
              || $ext eq 'css'
              || $ext eq 'php';

            $newHash{$cat}{$key} = $val;
        }
    }
    \%newHash;
}

1;
