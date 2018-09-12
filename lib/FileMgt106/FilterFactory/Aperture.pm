package FileMgt106::FilterFactory::Aperture;

# Copyright 2011-2017 Franck Latrémolière.
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

sub new {

    my ( $class, $scalar ) = @_;

    my $meta = delete $scalar->{'/FilterFactory::Aperture'};

    if ( my $starsById = delete $meta->{starsById} ) {
        $scalar->{'/filter'} = sub {
            my ( $k, $o, $minStars, $maxStars ) = @_;
            return unless $o;
            my $n;
            foreach my $a ( eval { keys %$o; } ) {
                foreach my $b ( eval { keys %{ $o->{$a} }; } ) {
                    foreach my $c ( eval { keys %{ $o->{$a}{$b} }; } ) {
                        foreach my $d ( eval { keys %{ $o->{$a}{$b}{$c} }; } ) {
                            foreach
                              my $e ( eval { keys %{ $o->{$a}{$b}{$c}{$d} }; } )
                            {
                                my $s = $starsById->{$e};
                                $n->{$a}{$b}{$c}{$d}{$e} =
                                  $o->{$a}{$b}{$c}{$d}{$e}
                                  if defined $s
                                  && $s >= $minStars
                                  && $s <= $maxStars;
                            }
                        }
                    }
                }
            }
            $n ? ( $k => $n ) : ();
        };
    }
    else {
        $scalar->{'/filter'} = sub { @_; };
    }

    if ( my $starsByFile = delete $meta->{starsByFile} ) {
        $scalar->{'/filterM'} = sub {
            my ( $k, $o, $minStars, $maxStars ) = @_;
            return unless $o;
            my $n;
            foreach my $a ( eval { keys %$o; } ) {
                foreach my $b ( eval { keys %{ $o->{$a} }; } ) {
                    foreach my $c ( eval { keys %{ $o->{$a}{$b} }; } ) {
                        foreach my $d ( eval { keys %{ $o->{$a}{$b}{$c} }; } ) {
                            foreach
                              my $e ( eval { keys %{ $o->{$a}{$b}{$c}{$d} }; } )
                            {
                                my $s = $starsByFile->{$a}{$b}{$c}{$d}{$e};
                                $n->{$a}{$b}{$c}{$d}{$e} =
                                  $o->{$a}{$b}{$c}{$d}{$e}
                                  if defined $s
                                  && $s >= $minStars
                                  && $s <= $maxStars;
                            }
                        }
                    }
                }
            }
            $n ? ( $k => $n ) : ();
        };
    }
    else {
        $scalar->{'/filterM'} = sub { @_; };
    }

    bless $scalar, $class;

}

sub otherItems {
    my ($self) = @_;
    my %other = %$self;
    delete $other{$_}
      foreach
      qw(Aperture.aplib Database Info.plist Masters Previews Thumbnails),
      grep { m#^/#s; } keys %other;
    %other;
}

sub database {
    my ( $self, $minStars, $maxStars ) = @_;
    return (
        'Aperture.aplib' => $self->{'Aperture.aplib'},
        'Info.plist'     => $self->{'Info.plist'},
        Database         => $self->{Database},
    ) unless defined $minStars;
    my @apfiles = $self->{'/filter'}->(
        Versions => $self->{Database}{Versions},
        $minStars, $maxStars
    ) or return;
    my %db = %{ $self->{Database} };
    $db{apdb} = {};
    delete $db{$_}
      foreach grep { ref $db{$_} && ref $db{$_} ne 'HASH'; } keys %db;
    'Aperture.aplib' => $self->{'Aperture.aplib'},
      'Info.plist'   => $self->{'Info.plist'},
      Database       => { %db, @apfiles, };
}

sub masters {
    my ( $self, $minStars, $maxStars ) = @_;
    return ( Masters => $self->{Masters} ) unless defined $minStars;
    $self->{'/filterM'}->( Masters => $self->{Masters}, $minStars, $maxStars );
}

sub previews {
    my ( $self, $minStars, $maxStars ) = @_;
    return ( Previews => $self->{Previews} ) unless defined $minStars;
    $self->{'/filter'}->( Previews => $self->{Previews}, $minStars, $maxStars );
}

sub subLibraryScalar {
    my ( $self, @rules ) = @_;
    +{
        $self->database(@rules),   $self->masters(@rules),
        $self->otherItems(@rules), $self->previews(@rules),
    };
}

sub exploded {
    my ($self) = @_;
    my %exploded;
    foreach my $rating ( -1 .. 5 ) {
        if ( my ( $k, $v ) = $self->masters( $rating, $rating ) ) {
            my $w = _explodeByLcExtension($v);
            while ( my ( $e, $x ) = each %$w ) {
                $exploded{ $k . $rating . '_' . $e } = $x;
            }
        }
        if ( my %v = $self->database( $rating, $rating ) ) {
            $exploded{ 'apfiles' . $rating } = \%v;
        }
        if ( my ( $k, $v ) = $self->previews( $rating, $rating ) ) {
            $exploded{ $k . $rating } = $v;
        }
    }
    $exploded{'3to5.aplibrary'} = $self->subLibraryScalar( 3, 5 );
    $exploded{'0to5.aplibrary'} = $self->subLibraryScalar( 0, 5 );
    $exploded{'-1to5.aplibrary'} = $self->subLibraryScalar;
    \%exploded;
}

sub _explodeByLcExtension {
    my ($what) = @_;
    my %newHash;
    while ( my ( $key, $val ) = each %$what ) {
        if ( ref $val eq 'HASH' ) {
            my $exploded = _explodeByLcExtension($val);
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

1;
