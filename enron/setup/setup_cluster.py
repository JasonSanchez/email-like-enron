#!/usr/bin/env python
#
# This script is obviously not production quality, it's just a simple way
# to set up some stuff on a spark-ec2 cluster and attach some volumes of data.
#
import os
from optparse import OptionParser
import select

import boto
import boto.ec2

import paramiko


def get_instances(name):
    """
    Get instance id for instances whos names contain `name`.

    :param name: word that the instances names contain.
    :type name: string
    """
    instances = []
    for r in conn.get_all_instances():
        for i in r.instances:
            if i.state == 'running' and name in i.tags['Name']:
                instances.append(i.id)
    return instances


def create_and_attach_volumes(instances,
                              name='Enron',
                              vol_size=215,
                              zone='us-east-1a',
                              snap_id='snap-d203feb5'):
    """
    create Enron volumes and attach them to the
    instances.

    :param instances: list of instance id's
    :type instances: list
    """
    # could probably make all this async
    # conn.create_volume(size, zone, snapshot=None)
    volumes = []
    for i in range(len(instances)):
        vol = conn.create_volume(vol_size, zone, snapshot=snap_id)
        volumes.append(vol)
        volumes[i].add_tag('Name', name)

        # create a little wait loop to ensure that the
        # volume is created
        print 'Waiting for volume %d to be created' % i

        while vol.volume_state() == 'creating':
            vol.update()

        print 'Volume created'
        print 'Attaching to %s' % instances[i]

        volumes[i].attach(instances[i],
                          '/dev/sdh',
                          dry_run=False)

    return volumes


def configure_nodes(key, ip, script):
    """
    SSH into the master using key and IP,
    execute script.
    """
    ssh = paramiko.SSHClient()

    # ensure that host keys are added.
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip, key_filename=key, username='root', password=None)
    # put files on master
    sftp = ssh.open_sftp()
    sftp.put(script, 'setup.sh')
    sftp.close()

    # change permissions on script
    stdin, stdout, stderr = ssh.exec_command('chmod +x setup.sh')

    # execute the script
    stdin, stdout, stderr = ssh.exec_command('./setup.sh')

    # wait for script to finish
    while not stdout.channel.exit_status_ready():
        if stdout.channel.recv_ready():
            rl, wl, xl = select.select([stdout.channel], [], [], 0.0)
            if len(rl) > 0:
                print stdout.channel.recv(1024),

    # close things down
    print "Script complete"
    ssh.close()
    return


def delete_volumes(name):
    """
    Delete volumes containing name.

    :param name: word that the names of the volumes to be deleted contains
    :type name: string
    """
    volumes = conn.get_all_volumes()
    for v in volumes:
        # Lets not delete volumes that are unamed
        try:
            if name in v.tags['Name']:
                if v.status == 'available':
                    v.delete()
                else:
                    print 'Volume %s is %s' % (v.tags['Name'], v.status)
                    print 'Can not delete'

        except KeyError:
            continue


if __name__ == '__main__':

    # The region where the public data sets are contained.
    # which also needs to be where we happened to launch
    # the spark cluster.
    REGION = 'us-east-1'
    key = os.environ['KEY_205']
    # Script which will be executed on master
    script_to_execute = './setup_nodes.sh'

    parser = OptionParser()
    parser.add_option('-s', '--setup',
                      action='store_true',
                      dest='start',
                      help='Setup cluster.')
    parser.add_option('-q', '--quit',
                      action='store_false',
                      dest='start',
                      help='Teardown.')

    (options, args) = parser.parse_args()

    conn = boto.ec2.connect_to_region(REGION)
    if options.start is True:
        print 'Configuring cluster...'
        # could probably be changed to allow for
        # selectable cluster tags.
        instances = get_instances('w251-mids')
        volumes = create_and_attach_volumes(instances)
        # Get the master ip
        # could probably be changed to deal with multiple instances
        # named master.
        master = get_instances('master')
        ip = conn.get_only_instances(master)[0].ip_address
        # execute script over ssh on master
        # this is last because it mounts the ebs volumes.
        configure_nodes(key, ip, script_to_execute)

    if options.start is False:
        print 'Tearing Down...'
        delete_volumes('Enron')
