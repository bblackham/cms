#!/usr/bin/env python2
# -*- coding: utf-8 -*-

# Programming contest management system
# Copyright © 2010-2012 Giovanni Mascellani <mascellani@poisson.phc.unipi.it>
# Copyright © 2010-2012 Stefano Maggiolo <s.maggiolo@gmail.com>
# Copyright © 2010-2012 Matteo Boscariol <boscarim@hotmail.com>
# Copyright © 2012 Luca Wehrstedt <luca.wehrstedt@gmail.com>
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

"""This service exports every data about the contest that CMS
knows. The process of exporting and importing again should be
idempotent.

"""

import argparse
import os
import shutil
import simplejson as json
import tempfile
import codecs

import tarfile

from sqlalchemy.orm import ColumnProperty, RelationshipProperty
from sqlalchemy.types import Boolean, Integer, Float, String, DateTime, Interval

from cms import logger
from cms.db import ask_for_contest
from cms.db.FileCacher import FileCacher
from cms.db.SQLAlchemyAll import SessionGen, Contest, Submission

from cmscontrib import sha1sum
from cmscommon.DateTime import make_timestamp


def get_archive_info(file_name):
    """Return information about the archive name.

    file_name (string): the file name of the archive to analyze.

    return (dict): dictionary containing the following keys:
                   "basename", "extension", "write_mode"

    """
    ret = {"basename": "",
           "extension": "",
           "write_mode": "",
           }
    if not (file_name.endswith(".tar.gz")
            or file_name.endswith(".tar.bz2")
            or file_name.endswith(".tar")
            or file_name.endswith(".zip")):
        return ret

    if file_name.endswith(".tar"):
        ret["basename"] = os.path.basename(file_name[:-4])
        ret["extension"] = "tar"
        ret["write_mode"] = "w:"
    elif file_name.endswith(".tar.gz"):
        ret["basename"] = os.path.basename(file_name[:-7])
        ret["extension"] = "tar.gz"
        ret["write_mode"] = "w:gz"
    elif file_name.endswith(".tar.bz2"):
        ret["basename"] = os.path.basename(file_name[:-8])
        ret["extension"] = "tar.bz2"
        ret["write_mode"] = "w:bz2"
    elif file_name.endswith(".zip"):
        ret["basename"] = os.path.basename(file_name[:-4])
        ret["extension"] = "zip"
        ret["write_mode"] = ""

    return ret


class ContestExporter:
    """This service exports every data about the contest that CMS
    knows. The process of exporting and importing again should be
    idempotent.

    """
    def __init__(self, contest_id, export_target,
                 skip_submissions, skip_user_tests, light):
        self.contest_id = contest_id
        self.skip_submissions = skip_submissions
        self.skip_user_tests = skip_user_tests
        self.light = light

        # If target is not provided, we use the contest's name.
        if export_target == "":
            with SessionGen(commit=False) as session:
                contest = Contest.get_from_id(self.contest_id, session)
                self.export_target = "dump_%s.tar.gz" % contest.name
        else:
            self.export_target = export_target

        self.file_cacher = FileCacher()

    def run(self):
        """Interface to make the class do its job."""
        return self.do_export()

    def do_export(self):
        """Run the actual export code.

        """
        logger.operation = "exporting contest %d" % self.contest_id
        logger.info("Starting export.")

        export_dir = self.export_target
        archive_info = get_archive_info(self.export_target)

        if archive_info["write_mode"] != "":
            # We are able to write to this archive.
            if os.path.exists(self.export_target):
                logger.critical("The specified file already exists, "
                                "I won't overwrite it.")
                return False
            export_dir = os.path.join(tempfile.mkdtemp(),
                                      archive_info["basename"])

        logger.info("Creating dir structure.")
        try:
            os.mkdir(export_dir)
        except OSError:
            logger.critical("The specified directory already exists, "
                            "I won't overwrite it.")
            return False

        files_dir = os.path.join(export_dir, "files")
        descr_dir = os.path.join(export_dir, "descriptions")
        os.mkdir(files_dir)
        os.mkdir(descr_dir)

        with SessionGen(commit=False) as session:

            contest = Contest.get_from_id(self.contest_id, session)

            # Export files.
            logger.info("Exporting files.")
            files = contest.enumerate_files(self.skip_submissions,
                                            self.skip_user_tests,
                                            light=self.light)
            for _file in files:
                if not self.safe_get_file(_file,
                                          os.path.join(files_dir, _file),
                                          os.path.join(descr_dir, _file)):
                    return False

            # Export the contest in JSON format.
            logger.info("Exporting the contest in JSON format.")

            self.ids = {contest: "0"}
            self.queue = [contest]

            data = dict()
            i = 0
            while i < len(self.queue):
                obj = self.queue[i]
                data[self.ids[obj]] = self.export_object(obj)
                i += 1

            with open(os.path.join(export_dir, "contest.json"), 'w') as fout:
                json.dump(data, fout, indent=4, sort_keys=True)

        # If the admin requested export to file, we do that.
        if archive_info["write_mode"] != "":
            archive = tarfile.open(self.export_target,
                                   archive_info["write_mode"])
            archive.add(export_dir, arcname=archive_info["basename"])
            archive.close()
            shutil.rmtree(export_dir)

        logger.info("Export finished.")
        logger.operation = ""

        return True

    def get_id(self, obj):
        if obj not in self.ids:
            # We use strings because they'll be the keys of a JSON object
            self.ids[obj] = str(len(self.ids))
            self.queue.append(obj)

        return self.ids[obj]

    def export_object(self, obj):
        """Export the given object, returning a JSON-encodable dict

        The returned dict will contain a "_class" item (the name of the
        class of the given object), an item for each column property
        (with a value properly translated to a JSON-compatible type)
        and an item for each relationship property (which will be an ID
        or a collection of IDs).

        The IDs used in the exported dict aren't related to the ones
        used in the DB: they are newly generated and their scope is
        limited to the exported file only. They are shared among all
        classes (that is, two objects can never share the same ID, even
        if they are of different classes). The contest will have ID 0.

        If, when exporting the relationship, we find an object without
        an ID we generate a new ID, assign it to the object and append
        the object to the queue of objects to export.

        The self.skip_submissions flag controls wheter we export
        submissions (and all other objects that can be reached only by
        passing through a submission) or not.

        """
        cls = type(obj)

        data = {"_class": cls.__name__}

        for prp in cls._obj_props:
            col = prp.columns[0]
            col_type = type(col.type)

            val = getattr(obj, prp.key)
            if col_type in [Boolean, Integer, Float, String]:
                data[prp.key] = val
            elif col_type is DateTime:
                data[prp.key] = make_timestamp(val) if val is not None else None
            elif col_type is Interval:
                data[prp.key] = val.total_seconds() if val is not None else None
            else:
                raise RuntimeError("Unknown SQLAlchemy column type: %s" % col_type)

        for prp in cls._rel_props:
            other_cls = prp.mapper.class_

            # Skip submissions if requested
            if self.skip_submissions and other_cls is Submission:
                continue

            val = getattr(obj, prp.key)
            if isinstance(val, other_cls):
                data[prp.key] = self.get_id(val)
            elif isinstance(val, list):
                data[prp.key] = list(self.get_id(i) for i in val)
            elif isinstance(val, dict):
                data[prp.key] = dict((k, self.get_id(v)) for k, v in val.iteritems())
            else:
                raise RuntimeError("Unknown SQLAlchemy relationship type: %s" % type(val))

        return data

    def safe_get_file(self, digest, path, descr_path=None):
        """Get file from FileCacher ensuring that the digest is
        correct.

        digest (string): the digest of the file to retrieve.
        path (string): the path where to save the file.
        descr_path (string): the path where to save the description.

        return (bool): True if all ok, False if something wrong.

        """
        # First get the file
        try:
            self.file_cacher.get_file(digest, path=path)
        except Exception as error:
            logger.error("File %s could not retrieved from file server (%r)."
                         % (digest, error))
            return False

        # Then check the digest
        calc_digest = sha1sum(path)
        if digest != calc_digest:
            logger.critical("File %s has wrong hash %s."
                            % (digest, calc_digest))
            return False

        # If applicable, retrieve also the description
        if descr_path is not None:
            with codecs.open(descr_path, 'w', encoding='utf-8') as fout:
                fout.write(self.file_cacher.describe(digest))

        return True


def main():
    """Parse arguments and launch process.

    """
    parser = argparse.ArgumentParser(description="Exporter of CMS contests.")
    parser.add_argument("-c", "--contest-id", action="store", type=int,
                        help="id of contest to export")
    parser.add_argument("-s", "--skip-submissions", action="store_true",
                        help="don't export submissions, only contest data")
    parser.add_argument("-t", "--skip-user-tests", action="store_true",
                        help="don't export user tests, only contest data")
    parser.add_argument("-l", "--light", action="store_true",
                        help="light export (without executables and "
                        "testcases)")
    parser.add_argument("export_target", nargs='?', default="",
                        help="target directory or archive for export")

    args = parser.parse_args()

    if args.contest_id is None:
        args.contest_id = ask_for_contest()

    ContestExporter(contest_id=args.contest_id,
                    export_target=args.export_target,
                    skip_submissions=args.skip_submissions,
                    skip_user_tests=args.skip_user_tests,
                    light=args.light).run()


if __name__ == "__main__":
    main()
