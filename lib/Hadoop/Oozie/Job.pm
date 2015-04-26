
# $Id: Job.pm,v 1.9 2015/02/25 21:20:28 martin Exp $

package Hadoop::Oozie::Job;

use 5.006;
use strict;
use warnings;

our
$VERSION = do { my @r = (q$Revision: 1.9 $ =~ /\d+/g); sprintf "%d."."%03d" x $#r, @r };

=head1 NAME

Hadoop::Oozie::Job - create and run remote Oozie jobs

=head1 SYNOPSIS

  use Hadoop::Oozie::Job;
  my $oHOJ = new Hadoop::Oozie::Job;
  $oHOJ->properties(
    {
    nameNode => q{hdfs://my.hadoop.cluster:8020},
    jobTracker => q{my.hadoop.cluster:8021},
    oozieProjectRoot => q{/hdfs/path/to/my/files},
    });
  $oHOJ->add_files(qw( local/path/workflow.xml local/path/mymapper.pl local/path/myreducer.py ));
  $oHOJ->oozie_host('my.oozie.server');
  my $sJobID = $oHOJ->run;
  do
    {
    sleep 5;
    my $sStatus = $oHOJ->status;
    } until ($sStatus eq 'SUCCEEDED');

=head1 DESCRIPTION

Encapsulates an Oozie job which you configure,
submit to a remote Oozie server (via the REST API),
run,
and inquire the status of.
Also allows you to "attach" files to the job
which will be uploaded to HDFS when the job is submitted
(these files will typically be mapper and reducer code,
and a workflow description file in XML format).

Note: this module has only been tested on simple map-reduce jobs in Hadoop streaming mode.

=cut

# Global variables and constants:
our $sRestPath  = q{/oozie/v1/job};
our $sRestPaths = q{/oozie/v1/jobs};

use Carp;
use Class::MakeMethods::Standard::Hash
  (
   'new' => 'new',
   'scalar' => [ 'oozie_host',  'oozie_port', ],
   'scalar' => ['httpfs_host', 'httpfs_port', ],
   'scalar' => ['properties', 'oozieProjectRoot', ],
  );
use Data::Dumper;
use File::Slurp;
use JSON;
use Net::Hadoop::WebHDFS;
use REST::Client;

=head1 SUBROUTINES/METHODS

=head2 new

Creates a new object of this type.
Does not take any arguments;
all object configuration is done by calling the various methods.

=head2 oozie_host($s)

Specify the hostname or IP address where your Oozie server is running.

=head2 oozie_port($i)

Specify the port number where your Oozie REST server is listening.
Default is 11000.

=head2 httpfs_host($s)

Specify the hostname or IP address where your HttpFS server is running.
You must call this method if you have files to be uploaded with the job.

=head2 httpfs_port($i)

Specify the port number where your HttpFS server is listening.
Default is 14000.

=head2 properties($hashref)

Specify the job properties,
either by a hashref of property.name => value pairs,
or full path to a local XML file,
or a string of pre-formed XML.
Does not do any checking of the XML.
For a complete example of what the XML should look like,
look at the config.xml at http://blog.cloudera.com/blog/2013/06/how-to-use-the-apache-oozie-rest-api/ 

=cut

sub _properties_XML
  {
  my $self = shift;
  my $raw = $self->properties;
  my $sXML = q{};
  if (ref($raw) eq 'HASH')
    {
    warn " DDD job properties is a hashref...";
    # Convert hash to well-formed XML.  Hopefully, order does not
    # matter:
    my $sTXT = q{};
    while (my ($sKey,$sVal) = each %$raw)
      {
      $sXML .= <<ENDPROP;
  <property>
    <name>$sKey</name>
    <value>$sVal</value>
  </property>
ENDPROP
      $sTXT .= qq{$sKey=$sVal\n};
      # Special cases:
      if ($sKey eq 'oozieProjectRoot')
        {
        $self->oozieProjectRoot($sVal);
        } # if
      } # while
    $sXML = qq{<configuration>\n$sXML</configuration>};
    } # if hashref
  # Not a hashref, assume we were passed a scalar
  elsif ($raw !~ m/</)
    {
    warn " DDD job properties is a file name...";
    # Scalar contains no < character, cannot be XML, assume it's a
    # local path:
    $sXML = read_file($raw, err_mode => 'carp');
    if (! defined $sXML)
      {
      # Did not read XML from local file.
      # TODO: set/return error condition
      return;
      } # if
    } # if
  else
    {
    # Assume the user passed in their own XML.  Fall through and just
    # use it:
    }
  return $sXML;
  } # _properties_XML

=head2 oozieProjectRoot($s)

Specify the HDFS directory where all this job's files will reside.
If you give this as one of the values in the hashref passed to the properties() method,
you do not need to also call oozieProjectRoot().

=head2 add_files(@as)

Add a list of files (local full path) which will be
copied to HDFS (into the oozieProjectRoot directory) when the job is submitted.

=cut

sub add_files
  {
  my $self = shift;
 FILE_TO_ADD:
  foreach my $sFile (@_)
    {
    if (! -f $sFile)
      {
      carp "$sFile does not exist as a file, ignoring";
      next FILE_TO_ADD;
      } # if
    push @{$self->{_as_files_}}, $sFile;
    } # foreach FILE_TO_ADD
  } # add_files

sub _send_files_to_hdfs
  {
  my $self = shift;
  my $oHDFS = new Net::Hadoop::WebHDFS(
                                       host => $self->httpfs_host,
                                       port => $self->httpfs_port,
                                       httpfs_mode => 1,
                                       # TODO: make this selectable:
                                       username => q{martin},
                                      );
  my $sDnameHDFS = $self->oozieProjectRoot;
  if (! $sDnameHDFS)
    {
    carp " EEE you need to call oozieProjectRoot() or properties() first\n";
    } # if
 FILE:
  foreach my $sPathLocal (@{$self->{_as_files_}})
    {
    my ($sPath, $sFile) = ($sPathLocal =~ m!\A(.+)[\\/](.+?)\z!);
    my $s = read_file($sPathLocal, binmode => ':raw') || warn " EEE error reading file $sPathLocal";
    my $sPathRemote = qq{$sDnameHDFS/$sFile};
    warn " DDD upload $sPathLocal to $sPathRemote...";
    eval { $oHDFS->create($sPathRemote, $s, overwrite => 'true') };
    if ($@)
      {
      warn " WWW cannot create HDFS file $sPathRemote: $@";
      } # if
    } # foreach SRC_FILE
  } # _send_files_to_hdfs

=head2 submit

Submit a fully-configured job to the Oozie server.
Does not run the job.
Returns the job ID.

=cut

sub submit
  {
  my $self = shift;
  my $sPropsXML = $self->_properties_XML;
  $self->_send_files_to_hdfs;
  my $sJSON = $self->_rest_post($sPropsXML);
  # warn $sJSON;
  my $rh;
  eval { $rh = decode_json($sJSON) };
  if ($@)
    {
    carp " EEE submit failed: $@";
    } # if
  # warn Dumper($rh);
  return $rh->{id};
  } # submit

sub _create_rest_client
  {
  my $self = shift;
  return if ref $self->{_o_rc_};
  $self->{_o_rc_} = new REST::Client;
  $self->{_o_rc_}->setHost(q{http://}. $self->oozie_host .q{:}. $self->oozie_port);
  # TODO: if this breaks, we might need to clear headers before every request:
  $self->{_o_rc_}->addHeader('Content-Type' => 'application/xml;charset=UTF-8');
  } # _create_rest_client

sub _rest_get
  {
  my $self = shift;
  my $sJobID = shift || return;
  # Remaining args are used as CGI-like parameters.
  $self->_create_rest_client;
  my $oU = URI->new(qq{$sRestPath/$sJobID});
  $oU->query_form(\@_);
  warn " DDD GETting $oU";
  $self->{_o_rc_}->GET($oU);
  # TODO: check for REST error?
  return $self->{_o_rc_}->responseContent;
  } # _rest_get

sub _rest_post
  {
  my $self = shift;
  my $sContent = shift || return;
  $self->_create_rest_client;
  my $i = length($sContent);
  warn " DDD POSTing $i bytes to $sRestPaths...";
  $self->{_o_rc_}->POST($sRestPaths, $sContent);
  # TODO: check for REST error?
  return $self->{_o_rc_}->responseContent;
  } # _rest_post

sub _rest_put
  {
  my $self = shift;
  my $sJobID = shift || return;
  my $sAction = shift || return;
  $self->_create_rest_client;
  my $sURL = qq{$sRestPath/$sJobID?action=$sAction};
  warn " DDD PUTting $sURL";
  $self->{_o_rc_}->PUT($sURL);
  # TODO: check for REST error?
  return $self->{_o_rc_}->responseContent;
  } # _rest_put

=head2 start($sID)

After job has been submitted, this method starts it.
Takes one argument, a job ID as returned by the submit() method.

=cut

sub start
  {
  my $self = shift;
  my $sJobID = shift || return;
  $self->_rest_put($sJobID, 'start');
  # TODO: error checking?
  } # start

=head2 run

Same as doing submit() and immediately start().
Takes the same arguments as submit().

=cut

sub run
  {
  my $self = shift;
  my $sJobID = $self->submit(@_);
  $self->start($sJobID);
  # TODO: error checking?
  return $sJobID;
  } # run

=head2 status($sID)

Given the ID of a running job,
returns its status in the form of a single word in all capital letters.
Normally, this will be one of the following:
PREP, RUNNING, PAUSED, SUCCEEDED, KILLED, FAILED
although there are other possible rare values.
See the Oozie documentation for details.
Takes one argument, a job ID as returned by the submit() or run() method.

=cut

sub status
  {
  my $self = shift;
  my $sJobID = shift || return;
  my $sJSON = $self->_rest_get($sJobID, show => 'info');
  # TODO: error-checking for JSON parse failure?
  my $rh = decode_json($sJSON);
  # warn $sJSON;
  # warn Dumper($rh);
  return $rh->{status};
  } # status

1;

__END__

=head1 AUTHOR

Martin Thurn, C<< <mthurn at cpan.org> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-hadoop-oozie-job at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Hadoop-Oozie-Job>.
I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.


=head1 SUPPORT

After installation, you can find documentation for this module with the perldoc command.

    perldoc Hadoop::Oozie::Job


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Hadoop-Oozie-Job>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Hadoop-Oozie-Job>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Hadoop-Oozie-Job>

=item * Search CPAN

L<http://search.cpan.org/dist/Hadoop-Oozie-Job/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright (C) 2015 Martin Thurn.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See L<http://dev.perl.org/licenses/> for more information.


=cut
