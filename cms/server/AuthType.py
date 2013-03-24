#!/usr/bin/env python2
# -*- coding: utf-8 -*-

# Programming contest management system
# Copyright © 2013 Bernard Blackham <b-cms@largestprime.net>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""This file contains the basic infrastructure from which we can
build an authentication type.

"""

from tornado.template import Template

class AuthType:
    TEMPLATE = ""

    @classmethod
    def get_login_html(self, **kwargs):
        """Return an HTML string inserted into the login page for this
        authentication method.

        return (string): an HTML string for the login page.

        """
        return Template(self.TEMPLATE).generate(**kwargs)

    @staticmethod
    def get_url_handlers():
        return []

    @staticmethod
    def get_application_params():
        return {}

    @staticmethod
    def get_user_string(user):
        return user.username
