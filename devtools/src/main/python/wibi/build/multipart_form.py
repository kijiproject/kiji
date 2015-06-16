#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""
This module is taken and modified from an online post http://pymotw.com/2/urllib2/. I'm still
not convinced this is the best way to do it, but it does work. I have been looking in to using
the email module to possibly achieve the same functionality.

TODO DEV-424: Merge functionality with rb_wrapper where appropriate.
TODO DEV-428: Determine if we want to use requests and possibly eliminate this class.
"""

import base64
import itertools
import mimetypes


class MultipartForm(object):
    """Accumulate the data to be used when posting a form."""
    def __init__(self):
        self.form_fields = []
        self.files = []
        self.boundary = '------WebKitFormBoundarygRqiIbZ8vPJcJlVf'
        return

    def get_content_type(self):
        return 'multipart/form-data; boundary=%s' % self.boundary

    def add_field(self, name, value):
        """Add a simple field to the form data."""
        self.form_fields.append((name, value))
        return

    def add_file(self, fieldname, filename, file_bytes, mimetype=None):
        """Add a file to be uploaded."""
        body = base64.b64encode(file_bytes)
        body = body.decode('utf-8')

        if mimetype is None:
            mimetype = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
        self.files.append((fieldname, filename, mimetype, body))
        return

    def __str__(self):
        """Return a string representing the form data, including attached files."""
        # Build a list of lists, each containing "lines" of the
        # request.  Each part is separated by a boundary string.
        # Once the list is built, return a string where each
        # line is separated by '\r\n'.
        parts = []
        part_boundary = '--' + self.boundary

        # Add the form fields
        for name, value in self.form_fields:
            parts.append([
                part_boundary,
                'Content-Disposition: form-data; name="%s"' % name,
                '',
                value,
            ])

        # Add the files to upload
        for field_name, filename, content_type, body in self.files:
            parts.append([
                part_boundary,
                'Content-Disposition: file; name="%s"; filename="%s"' % \
                (field_name, filename),
                'Content-Type: %s' % content_type,
                '',
                body,
            ])

        # Flatten the list and add closing boundary marker,
        # then return CR+LF separated data
        flattened = list(itertools.chain(*parts))
        flattened.append('--' + self.boundary + '--')
        flattened.append('')
        return '\r\n'.join(flattened)
