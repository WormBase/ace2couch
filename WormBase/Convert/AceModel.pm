package WormBase::Convert::AceModel;

# converts Ace models into CouchDB views

# namespace change: WormBase::Convert, i.e. WormBase::Convert::AceModel ?

use common::sense;
use Ace;
use JSON; # XS used if exists
use Exporter 'import';

our @EXPORT    = qw(model2designdocs model2pathdocs get_models);
our @EXPORT_OK = qw(list_models);

my %standard_views = (
#     class => <<'SUB',
# sub {
#     my $doc = shift;
#     dmap($doc->{_id} => $doc->{class});
# }
# SUB
#     name => <<'SUB',
# sub {
#     my $doc = shift;
#     dmap($doc->{_id} => $doc->{name});
# }
# SUB
    id => <<'SUB',
sub {
    my $doc = shift;
    dmap($doc->{_id} => undef);
}
SUB
);

use constant SUB_TEMPLATE => <<'SUB';
sub {
    my ($doc) = @_;
    if (__PREPATH__( my $href = $doc->__PATH__ )) {
        dmap($doc->{_id} => [keys %$href]);
    }
}
SUB

use constant SUB_TREE_TEMPLATE => <<'SUB';
sub {
    my ($doc) = @_;
    if (__PREPATH__( my $href = $doc->__PATH__ )) {
        dmap($doc->{_id} => $href);
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
        say $model->name;
        for my $ddoc (model2designdocs($model)) {
            say encode_json($ddoc);
        }
    }

    foreach my $model (get_models($dbh)) {
        say $model->name;
        for my $pathdoc (model2pathdocs($model)) {
            say encode_json($pathdoc);
        }
    }
}

sub model2pathdocs {
    my $model = shift;
    my $class = $model->name;
    die 'assert failure, model has unknown chars :', $class
        if $class =~ /[^A-Za-z0-9_]/;

    my $pathdoc = { _id => $class };
    for my $tag ($model->tags) {
        warn "For some reason, $tag is not a valid tag.\n"
            unless $model->valid_tag($tag);

        $pathdoc->{$tag} = [ $model->path($tag) ];
    }

    return $pathdoc;
}

sub model2designdocs {
    my $model = shift;
    my $class = $model->name;
    die 'assert failure, model has unknown chars :', $class
        if $class =~ /[^A-Za-z0-9_]/;

    my $tag_ddoc = {
        _id      => '_design/tag',
        language => 'perl',
    };
    my $tree_ddoc = {
        _id      => '_design/tree',
        language => 'perl',
    };

    # put the views in each ddoc
    for my $view_id (keys %standard_views) {
        $tag_ddoc->{views}->{$view_id}->{map}  = $standard_views{$view_id};
        $tree_ddoc->{views}->{$view_id}->{map} = $standard_views{$view_id};
    }

    # each tag is a "view"
    for my $tag ($model->tags) {
        warn "For some reason, $tag is not a valid tag.\n"
            unless $model->valid_tag($tag);
        my @path;
        my $prepath_string = join '', 
                             map { push(@path, "{'tag~$_'}"); 
                                   "\$doc->" . join('->', @path) . " && " } 
                             $model->path($tag);
        my $path_string = join '->', @path, "{'tag~$tag\'}";
        (my $sub_string = SUB_TEMPLATE) =~ s/__PREPATH__([\s\S]*)__PATH__/$prepath_string$1$path_string/g;
        (my $sub_tree_string = SUB_TREE_TEMPLATE) =~ s/__PREPATH__([\s\S]*)__PATH__/$prepath_string$1$path_string/g;

        $tag_ddoc->{views}->{$tag}->{map}  = $sub_string;
        $tree_ddoc->{views}->{$tag}->{map} = $sub_tree_string;
    }

    return ($tag_ddoc, $tree_ddoc);
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

__PACKAGE__
