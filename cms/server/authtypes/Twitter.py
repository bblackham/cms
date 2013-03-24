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


class Twitter(AuthType):
    TEMPLATE = """\
<form class="form-horizontal" action="{{ url_root }}/twitter_login" method="GET">
<div style="text-align: center;"><button type="submit" class="btn btn-primary btn-large">{{ _("Log in with Twitter") }}</button></div>
</form>"""

    @staticmethod
    def get_url_handlers():
        return [
            (r"/twitter_login", TwitterLoginHandler),
        ]

    @staticmethod
    def get_application_params():
        return {
            "twitter_consumer_key": config.twitter_consumer_key,
            "twitter_consumer_secret": config.twitter_consumer_secret,
        }

    @staticmethod
    def get_user_string(user):
        return "via Twitter"


class TwitterLoginHandler(BaseHandler, tornado.auth.TwitterMixin):
    """TwitterLogin handler.

    """
    @tornado.web.asynchronous
    def get(self):
        if self.get_argument("oauth_token", None):
            self.get_authenticated_user(self.async_callback(self._on_auth))
            return
        a = self.authenticate_redirect()

    def _on_auth(self, user_t):
        if not user_t:
            raise tornado.web.HTTPError(500, "Twitter auth failed")

        username = user_t["username"]
        user = self.sql_session.query(User)\
            .filter(User.auth_type == "Twitter")\
            .filter(User.contest == self.contest)\
            .filter(User.username == username).first()

        if user is None:
            user = User(user_t["name"], "",
                        username,
                        auth_type="Twitter", contest=self.contest)
            self.sql_session.add(user)
            self.sql_session.commit()

        self.try_user_login(user)
