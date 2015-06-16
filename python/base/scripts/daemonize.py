#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

import getpass
import logging
import os
import socket
import sys
import time
import traceback

from base import base
from base import cli
from base import command
from base import mail


LogLevel = base.LogLevel
ExitCode = base.ExitCode


class Error(Exception):
    """Errors raised in this module."""
    pass


# ------------------------------------------------------------------------------


MAX_RETRIES_MAIL_TEMPLATE = """\
Maximum number of retries (%(max_retries)d) reached \
for daemon running as %(user)s on %(host)s in directory %(work_dir)s :
%(command)s
"""


RESTART_MAIL_TEMPLATE = """\
Process exited with exit code %(exit_code)d \
for daemon running as %(user)s on %(host)s in directory %(work_dir)s :
%(command)s
--------------------
Output:
%(output)s
--------------------
Error:
%(error)s
--------------------
Daemon is being restarted.
"""


def SafeMail(subject, body, **kwargs):
    """Wraps mail.SendMail() in a way that may not fail.

    Logs the mail in case the mail system is not working properly.
    """
    try:
        mail.SendMail(subject=subject, body=body, **kwargs)
    except:
        traceback.print_exc()
        logging.error(
            'Error sending email: %r\n'
            'Subject: %r\n'
            'Body:\n'
            '----------------------------------------\n'
            '%s\n'
            '----------------------------------------',
            sys.exc_info(),
            subject
        )


class DaemonizeTool(cli.Action):
    """Runs a command as a daemon process.

    Automatically restarts the command if it stops.
    Sends email notifications.
    """

    def register_flags(self):
        self.flags.add_float(
            name='restart_cooldown_time_interval',
            default=10.0,
            help='Cool-down time interval before restarting a process, in seconds.',
        )

        self.flags.add_integer(
            name='max_restart',
            default=0,
            help=('Maximum number of restarts before giving up.\n'
                  'Zero or negative means no maximum (ie. restart forever).'),
        )

    def run(self, args):
        start_count = 0
        has_max_restarts = (self.flags.max_restart > 0)
        command_args = tuple(args)

        while True:
            try:
                cmd = None
                start_count += 1
                if has_max_restarts and (start_count > self.flags.max_restart):
                    logging.error('Maximum number of restarts reached, exiting!')

                    body = MAX_RETRIES_MAIL_TEMPLATE % dict(
                        max_retries=self.flags.max_restart,
                        command=' \\\n\t'.join(command_args),
                        user=getpass.getuser(),
                        host=socket.gethostname(),
                        work_dir=os.getcwd(),
                    )
                    SafeMail(subject='[FAILURE] Command: %s' % (command_args,), body=body)

                    return ExitCode.FAILURE

                logging.info('Running attempt #%d for command: %r', start_count, command_args)
                cmd = command.Command(args=args, start=False)
                cmd.Start(wait_for=False)
                logging.info('Attempt #%d running with PID %d', start_count, cmd.pid)
                cmd.WaitFor()

            except:
                logging.error('Error running command: %r', sys.exc_info()[0])
                traceback.print_exc()
            finally:
                if cmd is None:
                    logging.fatal('Error running command: %r', args)
                    return ExitCode.FAILURE
                else:
                    logging.error('Daemon exited with code: %d', cmd.exit_code)

                body = RESTART_MAIL_TEMPLATE % dict(
                    exit_code=cmd.exit_code,
                    command=' \\\n\t'.join(command_args),
                    user=getpass.getuser(),
                    host=socket.gethostname(),
                    work_dir=os.getcwd(),
                    output=cmd.output_text,
                    error=cmd.error_text,
                )
                SafeMail(subject='[FAILURE] Command: %s' % (command_args,), body=body)

                time.sleep(self.flags.restart_cooldown_time_interval)


# ------------------------------------------------------------------------------


def main(args):
    tool = DaemonizeTool(parent_flags=base.FLAGS)
    return tool(args)


if __name__ == '__main__':
    base.run(main)
