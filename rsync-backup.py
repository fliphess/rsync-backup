#!/usr/bin/env python

import argparse
import os
import sys
import paramiko
import socket
import subprocess
import yaml

import logging
import logging.handlers


def parse_options():
    parser = argparse.ArgumentParser(description="""

        Server backup script - Flip Hess 2014 - <flip@fliphess.com>
    """)
    parser.add_argument('-s', '--settings', help='Where to find the settings file', required=True, type=str)
    parser.add_argument('-v', '--verbosity', help='Show all debug output', action='count', default=0)
    arguments = vars(parser.parse_args())
    return arguments 


def logger(name, verbosity=1):
    global log 
    level = {
        0: logging.ERROR,
        1: logging.WARNING,
        2: logging.INFO,
        3: logging.DEBUG
    }.get(verbosity, logging.DEBUG)

    log = logging.getLogger(name=name)
    log.setLevel(level=level)

    fh = logging.FileHandler('/var/log/byte/backup-script.log')
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(level)

    formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)-7s %(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    log.addHandler(fh)
    log.addHandler(ch)
    return log


def rsync_dir(source, target, node, keyfile):
   ssh_options = ' -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'
   ssh_options += ' -i %s' % keyfile
   ssh_options += ' -x -o ConnectTimeout=1 -o PasswordAuthentication=no '
   ssh_cmd = "/usr/bin/ssh %s" % ssh_options

   rsync_cmd = "/usr/bin/rsync --recursive -av -e '%s' %s root@%s:%s 2>&1" % (ssh_cmd, source, node, target)

   process = subprocess.Popen(rsync_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
   while True:
       nextline = process.stdout.readline()
       if nextline == '' and process.poll() != None:
           break
       log.debug(nextline.strip())

   output = process.communicate()[0]
   exitCode = process.returncode

   if exitCode == 0:
       log.info('Rsync for %s to %s at %s [OK]' % (source, target, node))
       return True
   else:
       log.error('Rsync for %s to %s at %s: exitcode: %s [FAILED]' % (source, target, node, exitCode))
       log.debug('Output was: %s' % output)
       return False


def test_ssh(node, keyfile, user='root'):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=node, username=user, key_filename=keyfile)
    try:
        ssh = client.get_transport().open_session()
        ssh.exec_command('uptime')
        if ssh.recv_exit_status() != 0:
            return False
        else:
            return True
    except Exception as e:
        log.error('Testing ssh gave an exception: %s' % e)
        return False


def main():
    args = parse_options()
    log = logger("Backup script", args['verbosity'])

    log.info('Reading settings file %s' % args['settings'])
    try:
        with open(args['settings']) as fh:
            settings = yaml.load(fh)
    except Exception as e:
        log.error("Failed to parse settingsfile %s: Error: %s" % (args['settings'], e))

    if not os.path.isfile(settings['ssh_key']):
        log.error('SSH Keysfile %s not found!' % settings['ssh_key'])
        sys.exit(1)

    log.info('Testing remote server %s for ssh connectivity' % settings['backup_host'])
    ssh_ok = test_ssh(settings['backup_host'], settings['ssh_key'])
    if not ssh_ok:
        log.error('Failed to connect to %s' % settings['backup_host'])
        sys.exit(1)

    log.info('Starting backup routine')
    for source_dir in settings['backup_src']:
        if not os.path.isdir(source_dir):
            log.error('Dir %s not found! Skipping!' % source_dir)
            continue

        addendum = os.path.join(socket.getfqdn())
        target_dir = os.path.join(settings['backup_dest'], addendum)

        log.info('Rsyncing sourcedir %s to %s:%s' % (source_dir, settings['backup_host'], target_dir))
        rsync = rsync_dir(source_dir, target_dir, settings['backup_host'], settings['ssh_key'])
        if not rsync:
            log.error('Failed to connect to %s' % settings['backup_host'])
            sys.exit(1)
    log.info('All dirs done!')


if __name__ == "__main__":
    main()
