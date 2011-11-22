#!/usr/bin/perl
# accepts a Java formatted Ace dump of an object (STDIN/file) and
# converts it into a CouchDB JSON document (STDOUT)

use strict;
use warnings;
use Ace;
use JSON;
use URI::Escape::XS;

my $matrix = [];
my $treewidth = 0;
while (<>) {
    chomp;
    my @row = split /\t/;
    push @$matrix, \@row;
    $treewidth = @row if @row > $treewidth;
}

my $hash = make_hash($matrix, 0, 0, scalar(@$matrix), $treewidth);

my $key = (keys %$hash)[0];
$hash = $hash->{$key};
$hash->{_id} = uri_escape($key);

print encode_json($hash);

sub make_hash {
    my ($matrix, $row, $col, $maxrow, $maxcol) = @_;

    return unless $matrix->[$row][$col];

    my $hash;

    while ($row < $maxrow) {
        my $i = $row + 1;

        while ($i < $maxrow && !$matrix->[$i][$col]) { ++$i }

        my ($class, $name) = Ace->split($matrix->[$row][$col]);
        my $key = ucfirst $class eq $class ? "${class}_${name}" : $name;

        $hash->{$key}
            = make_hash($matrix, $row, $col + 1, $i, $maxcol);

        $row = $i;
    }

    return $hash;
}
