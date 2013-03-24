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

from cms import logger
from cms.server import filter_ascii
from cms.server.AuthType import AuthType
from cms.server.ContestWebServer import BaseHandler
from cms.db.SQLAlchemyAll import User


class Password(AuthType):
    TEMPLATE = """\
<form class="form-horizontal" action="{{ url_root }}/login" method="POST">
    <input type="hidden" name="next" value="{{ handler.get_argument("next", "/") }}">
    <fieldset>
        <div class="control-group">
            <label class="control-label" for="input01">{{ _("Username") }}</label>
            <div class="controls">
                <input type="text" class="input-xlarge" name="username">
            </div>
        </div>
        <div class="control-group">
            <label class="control-label" for="input01">{{ _("Password") }}</label>
            <div class="controls">
                <input type="password" class="input-xlarge" name="password">
            </div>
        </div>
        <div class="control-group">
            <div class="controls">
                <button type="submit" class="btn btn-primary btn-large">{{ _("Login") }}</button>
                <button type="reset" class="btn btn-large">{{ _("Reset") }}</button>
            </div>
        </div>
    </fieldset>
</form>"""

    @staticmethod
    def get_url_handlers():
        return [
            (r"/login", LoginHandler),
        ]


class LoginHandler(BaseHandler):
    """Login handler.

    """
    def post(self):
        username = self.get_argument("username", "")
        password = self.get_argument("password", "")
        user = self.sql_session.query(User)\
            .filter(User.auth_type == "Password")\
            .filter(User.contest == self.contest)\
            .filter(User.username == username).first()

        filtered_user = filter_ascii(username)
        filtered_pass = filter_ascii(password)
        if user is None or user.password != password:
            logger.info("Login error: user=%s pass=%s remote_ip=%s." %
                        (filtered_user, filtered_pass, self.request.remote_ip))
            self.redirect("/?login_error=true")
            return

        self.try_user_login(user)
