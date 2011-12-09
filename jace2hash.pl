#!/usr/bin/perl
# accepts a Java formatted Ace dump of an object (STDIN/file) and
# converts it into a CouchDB JSON document (STDOUT)

# thin wrapper for JAce converter

use WormBase::Convert::JAce qw(run); run;
