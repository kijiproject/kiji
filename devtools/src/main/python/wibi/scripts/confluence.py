#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-

"""Command-line tool to interact with Confluence Wiki."""

import getpass
import logging
import os
import sys
import tempfile
import xmlrpc.client

from xml.dom import minidom

from base import base
from base import cli

from wibi.scripts import kiji_config

# --------------------------------------------------------------------------------------------------


class Error(Exception):
    pass


# Confluence server configuration template:
CONFLUENCE_CONF_TEMPLATE = """\
# Short ID for the Confluence server:
id: %(id)s

# Base URL of the Confluence server:
url: %(url)s

# Login for this Confluence server:
login: %(login)s
"""


CONFLUENCE_CONTENT_TEMPLATE = """\
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE ac:confluence PUBLIC
    "-//Atlassian//Confluence 4 Page//EN"
    "http://www.atlassian.com/schema/confluence/4/confluence.dtd">
<ac:confluence
    xmlns:ac="http://www.atlassian.com/schema/confluence/4/ac/"
    xmlns:ri="http://www.atlassian.com/schema/confluence/4/ri/"
    xmlns="http://www.atlassian.com/schema/confluence/4/">
{}
</ac:confluence>
"""




class ConfluenceCLI(cli.Action):
    USAGE = """
    |Usage:
    |
    |    %(this)s [--login=<login>] [--editor=emacs] [--page-id=]<page-id>
    """

    def RegisterFlags(self):
        self.flags.add_string(
            name="page_id",
            default=None,
            help=("Page ID to edit.\n"
                  "The ID of a page can be determined in the Confluence URL, while editing a page."),
        )
        self.flags.add_string(
            name="editor",
            default="$EDITOR",
            help=("Editor command to use.\n"
                  "There is only one choice here: emacs."),
        )
        self.flags.add_string(
            name="server",
            default="https://wiki.wibidata.com",
            help="Base URL of the Confluence server to interact with.",
        )
        self.flags.add_boolean(
            name="pretty",
            default=True,
            help="When set, automatically prettify the XML through minidom.Element.toprettyxml().",
        )

    def Run(self, args):
        page_id = self.flags.page_id
        if (page_id is None) and (len(args) > 0):
            page_id, args = args[0], args[1:]
        assert (page_id is not None), ("Specify a page ID with [--page-id=]<page-id>")
        assert (len(args) == 0), ("Unexpected command-line arguments: %r" % args)

        self._conf = kiji_config.Configuration.Get()

        url = os.path.join(self.flags.server, "rpc/xmlrpc")
        self._client = xmlrpc.client.Server(url, verbose=0)

        auth_token = self._get_auth_token()
        logging.debug("Confluence auth token: %r", auth_token)

        # TODO: need a wrapper to perform re-authentication globally on all methods?

        # Read content:
        while True:
            try:
                page = self._client.confluence2.getPage(auth_token, page_id)
                break
            except xmlrpc.client.Fault as err:
                logging.error("Error during XMLRPC request: %r", err)
                auth_token = self._get_auth_token(force=True)

        # Normalize content:
        pretty = True

        content = page["content"]
        if pretty:
            content = CONFLUENCE_CONTENT_TEMPLATE.format(content)
            dom = minidom.parseString(content)
            xml_strip_text(dom)
            dom.normalize()
            content = dom.toprettyxml(indent="  ")

        # Modify content:
        logging.debug("Page JSON descriptor: %r", page)
        content = self.edit(content, prefix="confluence.%s" % page_id)

        # Strip the XHTML prefix/suffix:
        if pretty:
            content = "\n".join(content.splitlines()[5:-1])

        page["content"] = content

        # Write content:
        while True:
            try:
                result = self._client.confluence2.storePage(auth_token, page)
                logging.info("Wrote page %r version %s", page["title"], page["version"])
                logging.debug("Store page result: %r", result)
                break
            except xmlrpc.client.Fault as err:
                logging.error("Error during XMLRPC request: %r", err)
                auth_token = self._get_auth_token(force=True)


        # Do not logout so we don't have to authenticate all the time:
        # client.confluence2.logout(auth_token)

    def _get_auth_token(self, force=False):
        """Returns: an authentication token.

        Args:
            force: Force the acquisition of a new token.
        """
        server = self.flags.server

        config = self._conf.confluence.get(server)
        if config is None:
            config = base.InputTemplate(
                template=CONFLUENCE_CONF_TEMPLATE,
                fields=dict(
                    id=server,
                    url=server,
                    login=getpass.getuser(),
                )
            )
            self._conf.AddConfluence(config)
            self._conf.Write()

        if force:
            config["auth_token"] = None

        auth_token = config.get("auth_token")
        if auth_token is None:
            login = config["login"]
            password = getpass.getpass(
                "Password for user %r on Confluence server %r: " % (login, server))
            auth_token = self._client.confluence2.login(login, password)
            config["auth_token"] = auth_token
            self._conf.Write()

        logging.debug("Confluence auth token: %r", auth_token)
        return auth_token

    def edit(self, text, prefix="confluence"):
        """Opens the user's preferred editor to edit the given text.

        Args:
            text: Text to edit.
        Returns:
            The edited text, after the user closed the editor.
        """
        with tempfile.NamedTemporaryFile(prefix=prefix + ".", suffix=".xml", delete=False) as temp:
            file_path = temp.name
            logging.info("Editing %r", file_path)

            with open(file_path, "wt") as f:
                f.write(text)

            command = "%s %r" % (self.flags.editor, file_path)
            exit_code = os.system(command)
            assert (exit_code == 0), ("Error while opening a text editor: %r" % command)

            with open(file_path, "rt") as f:
                return f.read()



def xml_strip_text(node):
    """Strips text sections of the XML document."""
    if node.nodeType == minidom.Text.TEXT_NODE:
        node.data = node.data.strip()
    if node.prefix is not None:
        node.prefix = node.prefix.strip()

    for child in node.childNodes:
        xml_strip_text(child)


# --------------------------------------------------------------------------------------------------


def main(args):
    ccli = ConfluenceCLI(parent_flags=base.FLAGS)
    return ccli(args)


if __name__ == "__main__":
    base.Run(main)
