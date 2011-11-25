use strict;
use warnings;

use Coro;
use Coro::AnyEvent;
use AnyEvent;
use AnyEvent::CouchDB;
use Data::Dumper;

my $couch = AnyEvent::CouchDB->new; # localhost 5984
my $db = $couch->db('ws228_experimental');

my $ddocs = { map { $_->{_id} => $_ } map { $_->{doc} }
              @{get_all_views($db)->recv->{rows}} };


my @coros = map {
    my $ddoc_id= $_;
    async {
        print "Refreshing $ddoc_id\n";
        my ($arbitrary_view) = keys %{$ddocs->{$ddoc_id}->{views}};
        (my $model = $ddoc_id) =~ s{_design/}{};

        # try getting the view until it's generated
        my $result;
        while (!($result = eval { $db->view($model . '/' . $arbitrary_view)->recv })) {
            Coro::AnyEvent::sleep(10);
        };

        print "Refreshed $ddoc_id.\n";
        print Dumper($result);
    };
} sort keys %$ddocs;

$_->join foreach @coros;

sub get_all_views { # can probably subclass this
    my $db = shift;
    return $db->all_docs({
        startkey     => '_design/',
        endkey       => '_design/zzzzzzzzzzzzzz',
        include_docs => 1,
    });
}
