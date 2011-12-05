package WormBase::JaceConverter;

# modulino for converting Jace dumps

use strict;
use warnings;
use Ace;
use JSON;
use URI::Escape::XS qw(uri_escape);
use Exporter 'import';

our @EXPORT = qw(treematrix2hash);

sub run {
    my $matrix = [];
    while (<>) {
        chomp;
        push @$matrix, [split /\t/];
    }

    my $hash = treematrix2hash($matrix);

    my $key = (keys %$hash)[0];
    $hash = $hash->{$key};
    $hash->{_id} = uri_escape($key);

    print encode_json($hash);
}

sub treematrix2hash {
    my ($matrix, $row, $col, $maxrow, $maxcol) = @_;
    $row    //= 0;
    $col    //= 0;
    $maxrow //= scalar @$matrix;
    $maxcol //= _get_matrix_width($matrix);

    return if $col >= $maxcol;
    return unless $matrix->[$row][$col];

    my $hash;

    while ($row < $maxrow) {
        my $i = $row + 1;

        while ($i < $maxrow && !$matrix->[$i][$col]) { ++$i }

        my ($class, $name) = Ace->split($matrix->[$row][$col]);

        $hash->{"${class}~${name}"}
            = treematrix2hash($matrix, $row, $col + 1, $i, $maxcol);

        $row = $i;
    }

    return $hash;
}

sub _get_matrix_width {
    my $matrix = shift;
    my $width = 0;
    foreach (@$matrix) {
        $width = @$_ if @$_ > $width;
    }
    return $width;
}

run() unless caller;

1;
