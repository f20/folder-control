package FileMgt106::CLI::ScanCLI;

# Copyright 2011-2021 Franck Latrémolière and others.
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

use Encode qw(decode_utf8);
use FileMgt106::FileSystem qw(STAT_MTIME);

use constant {
    SCLI_START    => 0,
    SCLI_PERL5DIR => 1,
};

sub _prettifyField {
    my ( $number, $spaces ) = @_;
    $number = 'undef' unless defined $number;
    do { } while $number =~ s/([0-9])([0-9]{3})(?:,|$)/$1,$2/s;
    $spaces -= length $number;
    ( $spaces > 0 ? ' ' x $spaces : '' ) . $number;
}

sub scan_command_migrate {

    my ( $self, $command, $oldFileName ) = @_;

    $oldFileName = rel2abs( $oldFileName, $self->[SCLI_START] )
      if defined $oldFileName;
    chdir dirname( $self->[SCLI_PERL5DIR] )
      or die "chdir dirname($self->[SCLI_PERL5DIR]): $!";
    unless ( $oldFileName && -f $oldFileName ) {
        my $mtime = ( stat '~$hints' )[STAT_MTIME]
          or die 'No existing hints file in ' . `pwd`;
        require POSIX;
        $mtime = POSIX::strftime( '%Y-%m-%d %H-%M-%S %Z', localtime($mtime) );
        $oldFileName = '~$hints ' . $mtime;
        rename '~$hints', $oldFileName
          or die "Cannot move ~\$hints to $oldFileName: $!";
    }
    my $hints    = $self->hintsObj;
    my $dbHandle = $hints->{dbHandle};
    $dbHandle->{AutoCommit} = 1;
    $dbHandle->do( 'pragma journal_mode=' . ( /nowal/i ? 'delete' : 'wal' ) )
      if /wal/i;
    $dbHandle->do("attach '$oldFileName' as old");

    if ( $dbHandle->do('begin exclusive transaction') ) {
        my $reportProgress = sub {
            warn join( '',
                _prettifyField( $_[0], 5 ),
                map { _prettifyField( $_, 15 ); } @_[ 1 .. $#_ ] )
              . "\n";
        };
        $reportProgress->(qw(Level Added Total));
        $dbHandle->do('delete from main.locations');
        $hints->{deepCopy}->( 'old.locations', 0, 0, undef, $reportProgress );
        warn 'Committing changes';
        sleep 2 while !$dbHandle->commit;
        __PACKAGE__->new(@$self)->hintsObj
          or die 'Cannot complete database initialisation';
    }
    else {
        warn 'New database is in use: no migration done';
    }

}

sub scan_command_volume {

    my ( $self, $command, $subcommand, @volumes ) = @_;
    my $hints    = $self->hintsObj;
    my $dbHandle = $hints->{dbHandle};

    if ( defined $subcommand && $subcommand =~ /on|off|enab|disab/i ) {
        my $newparid = $subcommand =~ /on|enab/i ? 0 : -1;
        $hints->beginInteractive;
        $dbHandle->do( 'update locations set parid=? where parid<1 and name=?',
            undef, $newparid, $_ )
          foreach @volumes;
        $hints->commit;
    }

    my $reportInfo = sub {
        print join( '',
            $_[0],
            ' ' x ( 8 - length $_[0] ),
            ( map { _prettifyField( $_, 11 ); } @_[ 1 .. 3 ] ),
            ' ', $_[4] || '/',
        ) . "\n";
    };
    $reportInfo->( qw(Status Folders Files), 'Max MB', 'Volume' );
    my $q = $dbHandle->prepare( 'select parid, locid, name from locations'
          . ' where parid<1 order by parid desc, name' );
    my $qc =
      $dbHandle->prepare( 'select sum(size is null), sum(size is not null)'
          . ', CAST((sum(size)+99999)/1e6 AS INT)'
          . ' from locations where rootid=?' );
    $q->execute;
    while ( my ( $parid, $locid, $name ) = $q->fetchrow_array ) {
        $qc->execute($locid);
        $reportInfo->(
            $parid ? 'Disabled' : 'Enabled',
            $qc->fetchrow_array, decode_utf8 $name
        );
        $qc->finish;
    }

}

*scan_command_volumes = \&scan_command_volume;

1;
