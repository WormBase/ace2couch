#!/usr/bin/perl
# accepts a Java formatted Ace dump of an object (STDIN/file) and
# converts it into a CouchDB JSON document (STDOUT)

# thin wrapper for JaceConverter

use WormBase::JaceConverter ();
WormBase::JaceConverter::run();
