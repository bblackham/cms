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

from cms import config
from cms.server.AuthType import AuthType
from cms.server.ContestWebServer import BaseHandler
from cms.db.SQLAlchemyAll import User

import tornado.auth


class Facebook(AuthType):
    TEMPLATE = """\
<form class="form-horizontal" action="{{ url_root }}/facebook_login" method="GET">
<div style="text-align: center;"><button type="submit" class="btn btn-primary btn-large">{{ _("Log in with Facebook") }}</button></div>
</form>"""

    @staticmethod
    def get_url_handlers():
        return [
            (r"/facebook_login", FacebookLoginHandler),
        ]

    @staticmethod
    def get_user_string(user):
        return "via Facebook"


class FacebookLoginHandler(BaseHandler, tornado.auth.FacebookGraphMixin):
    """FacebookLogin handler.

    """
    @tornado.web.asynchronous
    def get(self):
        referer = self.request.headers.get("Referer")
        if referer is None:
            self.redirect("/")
            return
        base = referer.split("#")[0].split("?")[0].rstrip("/")
        uri = base + "/facebook_login"

        if self.get_argument("code", False):
            self.get_authenticated_user(
                redirect_uri=uri,
                client_id=config.facebook_app_id,
                client_secret=config.facebook_app_secret,
                code=self.get_argument("code"),
                callback=self.async_callback(self._on_login))
            return
        self.authorize_redirect(
            redirect_uri=uri,
            client_id=config.facebook_app_id)

    def _on_login(self, user_t):
        if not user_t:
            raise tornado.web.HTTPError(500, "Facebook auth failed")

        username = user_t["id"]
        user = self.sql_session.query(User)\
            .filter(User.auth_type == "Facebook")\
            .filter(User.contest == self.contest)\
            .filter(User.username == username).first()

        if user is None:
            user = User(user_t["first_name"], user_t["last_name"],
                        username,
                        auth_type="Facebook", contest=self.contest)
            self.sql_session.add(user)
            self.sql_session.commit()

        self.try_user_login(user)
