#!/usr/bin/perl
# runloadcouch.pl

# run NUM_FORKS loaders at a time

use common::sense;
use File::Spec;
use Parallel::ForkManager;

use constant NUM_FORKS => 1; # number of cores available
my $EXTENSION = ".jace";
my $DB_PREFIX = "ws233_";

my $dir = shift or die "Need directory\n";
unless (-e $dir and -d $dir) {
    die "$dir is not a directory\n";
}

my $pm = Parallel::ForkManager->new(NUM_FORKS);

mkdir 'logs';
mkdir 'err';

my $cmd;
$cmd = qq(perl loadmodels.pl "${DB_PREFIX}_model");
system($cmd);

opendir(my $dirh, $dir);
my @files = sort readdir($dirh);
for my $fname (@files) {
    my $base = $fname;
    next unless $base =~ s/\Q$EXTENSION\E(-\d+)?$/$1/;
    (my $model = $fname) =~ s/\Q$EXTENSION\E(?:-\d+)?$//;

    my $file = File::Spec->catfile($dir, $fname);

    $pm->start and next;

    $cmd = qq(perl loadviews.pl "${DB_PREFIX}\L${base}\E" "$model");
    print "Loading views for $base into $DB_PREFIX\L$base\E\n";
    system($cmd);
    print "Loaded views for $base\n";

    $cmd = qq(perl loadcouch.pl --db "${DB_PREFIX}\L${base}\E" --q )
         . qq(@ARGV < "$file" > "logs/$fname.log" 2> "err/$fname.err");
    print "Loading objects for $base into $DB_PREFIX\L$base\E\n";
    system($cmd);
    print "Loaded objects for $base\n";

    $pm->finish;
}

$pm->wait_all_children;
