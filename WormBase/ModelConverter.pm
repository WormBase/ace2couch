package WormBase::ModelConverter;

# converts Ace models into CouchDB views

# namespace change: WormBase::Convert, i.e. WormBase::Convert::AceModel ?

use strict;
use warnings;
use Ace;
use JSON;
use Exporter 'import';

our @EXPORT    = qw(model2designdoc get_models);
our @EXPORT_OK = qw(list_models);

my %standard_views = (
    class => <<'SUB',
sub {
    my $doc = shift;
    dmap([$doc->{_id}, $doc->{class}, $doc->{name} => $doc->{class});
}
SUB
    name => <<'SUB',
sub {
    my $doc = shift;
    dmap([$doc->{_id}, $doc->{class}, $doc->{name}] => $doc->{name});
}
SUB
);

use constant SUB_TEMPLATE => <<'SUB';
sub {
    my ($doc) = @_;
    if (my $href = $doc->__PATH__) {
        dmap([$doc->{_id}, $doc->{class}, $doc->{name}] => [keys %$href]);
    }
}
SUB

use constant SUB_TREE_TEMPLATE => <<'SUB';
sub {
    my ($doc) = @_;
    if (my $href = $doc->__PATH__) {
        dmap([$doc->{_id}, $doc->{class}, $doc->{name}] => $href);
    }
}
SUB

sub run {
    require Getopt::Long;
    my ($host, $port) = ('localhost', 2005);
    Getopt::Long::GetOptions('host=s' => \$host, 'port=i' => \$port);

    my $dbh = Ace->connect(-host => $host, -port => $port)
        or die 'Connection error: ', Ace->error;

    foreach my $model (get_models($dbh)) {
        print $model->name, "\n";
        my $ddoc = model2designdoc($model);
        print encode_json($ddoc);
    }
}

sub model2designdoc {
    my $model = shift;
    my $class = $model->name;
    die 'assert failure, model has unknown chars :', $class
        if $model->name =~ /[^A-Za-z0-9_]/;
    my $ddoc = {
        _id      => '_design/' . lc $class,
        language => 'perl',
        views    => {},
    };

    for my $view_id (keys %standard_views) {
        $ddoc->{views}->{$view_id}->{map} = $standard_views{$view_id};
    }

    ## each tag is a "view"
    for my $tag ($model->tags) {
        warn "For some reason, $tag is not a valid tag.\n"
            unless $model->valid_tag($tag);

        my $path_string = join '->',
                          map { "{'" . $_ . "'}" }
                          $model->path($tag), $tag;

        (my $sub_string = SUB_TEMPLATE) =~ s/__PATH__/$path_string/g;
        (my $sub_tree_string = SUB_TREE_TEMPLATE) =~ s/__PATH__/$path_string/g;

        $ddoc->{views}->{$tag}->{map} = $sub_string;
        $ddoc->{views}->{"${tag}_TREE"}->{map} = $sub_tree_string;
    }

    return $ddoc;
}

# requires DB handle
sub get_models {
    my $dbh = shift or return;
    return grep defined, map { $dbh->model($_) } list_models($dbh);
}

# requires DB handle
sub list_models {
    my $dbh = shift or return;
    $dbh->raw_query('find Model');
    my $raw_data = $dbh->raw_query('list');

    my @models;

    open my $data, '<', \$raw_data;
    while (<$data>) {
        chomp;
        next unless s/^\s+\?(.*)/$1/;
        push @models, $_;
    }

    return @models;
}

run() unless caller;

1;
