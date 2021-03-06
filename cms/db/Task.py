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

"""Task-related database interface for SQLAlchemy. Not to be used
directly (import it from SQLAlchemyAll).

"""

from sqlalchemy.schema import Column, ForeignKey, ForeignKeyConstraint, \
    CheckConstraint, UniqueConstraint
from sqlalchemy.types import Boolean, Integer, Float, String, Interval
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.orderinglist import ordering_list

from cms.db.SQLAlchemyUtils import Base
from cms.db.Contest import Contest
from cms.db.SmartMappedCollection import smart_mapped_collection

from datetime import timedelta


class Task(Base):
    """Class to store a task. Not to be used directly (import it from
    SQLAlchemyAll).

    """
    __tablename__ = 'tasks'
    __table_args__ = (
        UniqueConstraint('contest_id', 'num',
                         name='cst_task_contest_id_num'),
        UniqueConstraint('contest_id', 'name',
                         name='cst_task_contest_id_name'),
        CheckConstraint("token_initial <= token_max"),
        )

    # Auto increment primary key.
    id = Column(
        Integer,
        primary_key=True,
        autoincrement='ignore_fk')

    # Number of the task for sorting.
    num = Column(
        Integer,
        nullable=False)

    # Contest (id and object) owning the task.
    contest_id = Column(
        Integer,
        ForeignKey(Contest.id,
                   onupdate="CASCADE", ondelete="CASCADE"),
        nullable=False,
        index=True)
    contest = relationship(
        Contest,
        backref=backref('tasks',
                        collection_class=ordering_list('num'),
                        order_by=[num],
                        cascade="all, delete-orphan",
                        passive_deletes=True))

    # Short name and long human readable title of the task.
    name = Column(
        String,
        nullable=False)
    title = Column(
        String,
        nullable=False)

    # A JSON-encoded lists of strings: the language codes of the
    # statments that will be highlighted to all users for this task.
    primary_statements = Column(
        String,
        nullable=False,
        default="[]")

    # Parameter to define the token behaviour. See Contest.py for
    # details. The only change is that these parameters influence the
    # contest in a task-per-task behaviour. To play a token on a given
    # task, a user must satisfy the condition of the contest and the
    # one of the task.
    token_initial = Column(
        Integer,
        CheckConstraint("token_initial >= 0"),
        nullable=True)
    token_max = Column(
        Integer,
        CheckConstraint("token_max > 0"),
        nullable=True)
    token_total = Column(
        Integer,
        CheckConstraint("token_total > 0"),
        nullable=True)
    token_min_interval = Column(
        Interval,
        CheckConstraint("token_min_interval >= '0 seconds'"),
        nullable=False,
        default=timedelta())
    token_gen_time = Column(
        Interval,
        CheckConstraint("token_gen_time >= '0 seconds'"),
        nullable=False,
        default=timedelta())
    token_gen_number = Column(
        Integer,
        CheckConstraint("token_gen_number >= 0"),
        nullable=False,
        default=0)

    # Maximum number of submissions or user_tests allowed for each user
    # on this task during the whole contest or None to not enforce
    # this limitation.
    max_submission_number = Column(
        Integer,
        CheckConstraint("max_submission_number > 0"),
        nullable=True)
    max_user_test_number = Column(
        Integer,
        CheckConstraint("max_user_test_number > 0"),
        nullable=True)

    # Minimum interval between two submissions or user_tests for this
    # task, or None to not enforce this limitation.
    min_submission_interval = Column(
        Interval,
        CheckConstraint("min_submission_interval > '0 seconds'"),
        nullable=True)
    min_user_test_interval = Column(
        Interval,
        CheckConstraint("min_user_test_interval > '0 seconds'"),
        nullable=True)

    # The scores for this task will be rounded to this number of
    # decimal places.
    score_precision = Column(
        Integer,
        CheckConstraint("score_precision >= 0"),
        nullable=False,
        default=0)

    # Active data set currently being used for scoring.
    active_dataset_id = Column(
        Integer,
        ForeignKey('datasets.id',
                   onupdate="SET DEFAULT", ondelete="SET DEFAULT",
                   use_alter=True,
                   name='fk_active_dataset_id'),
        nullable=True)

    # Provide easy access to the active dataset object.
    active_dataset = relationship(
        'Dataset',
        primaryjoin='and_(Task.active_dataset_id==Dataset.id)',
        post_update=True,
        uselist=False,
        single_parent=True,
        cascade='all, delete-orphan',
        )

    # Follows the description of the fields automatically added by
    # SQLAlchemy.
    # submission_format (list of SubmissionFormatElement objects)
    # datasets (dict of Dataset objects indexed by id)
    # attachments (dict of Attachment objects indexed by filename)
    # statements (dict of Statement objects indexed by language code)
    # submissions (list of Submission objects)
    # user_tests (list of UserTest objects)

    # This object (independent from SQLAlchemy) is the instance of the
    # ScoreType class with the given parameters, taking care of
    # building the scores of the submissions.
    scorer = None

    def export_to_dict(self):
        """Return object data as a dictionary.

        """
        return {'name':                 self.name,
                'title':                self.title,
                'num':                  self.num,
                'statements':           [statement.export_to_dict()
                                         for statement
                                         in self.statements.itervalues()],
                'attachments':          [attachment.export_to_dict()
                                         for attachment
                                         in self.attachments.itervalues()],
                'primary_statements':   self.primary_statements,
                'submission_format':    [element.export_to_dict()
                                         for element
                                         in self.submission_format],
                'active_dataset': self.active_dataset_id,
                'datasets': [dataset.export_to_dict()
                             for dataset in self.datasets],
                'token_initial':        self.token_initial,
                'token_max':            self.token_max,
                'token_total':          self.token_total,
                'token_min_interval':
                    self.token_min_interval.total_seconds(),
                'token_gen_time':
                    self.token_gen_time.total_seconds(),
                'token_gen_number':     self.token_gen_number,
                'max_submission_number': self.max_submission_number,
                'max_user_test_number': self.max_user_test_number,
                'min_submission_interval':
                    self.min_submission_interval.total_seconds()
                    if self.min_submission_interval is not None else None,
                'min_user_test_interval':
                    self.min_user_test_interval.total_seconds()
                    if self.min_user_test_interval is not None else None,
                'score_precision':      self.score_precision,
                }

    @classmethod
    def import_from_dict(cls, data):
        """Build the object using data from a dictionary.

        """
        data['attachments'] = [Attachment.import_from_dict(attch_data)
                               for attch_data in data['attachments']]
        data['attachments'] = dict([(attachment.filename, attachment)
                                    for attachment in data['attachments']])
        data['submission_format'] = [SubmissionFormatElement.import_from_dict(
            sfe_data) for sfe_data in data['submission_format']]
        data['datasets'] = [Dataset.import_from_dict(dataset_data)
                            for dataset_data in data['datasets']]
        if data['active_dataset'] is not None:
            data['active_dataset'] = data['datasets'][data['active_dataset']]
        data['statements'] = [Statement.import_from_dict(statement_data)
                              for statement_data in data['statements']]
        data['statements'] = dict([(statement.language, statement)
                                   for statement in data['statements']])
        if 'token_min_interval' in data:
            data['token_min_interval'] = \
                timedelta(seconds=data['token_min_interval'])
        if 'token_gen_time' in data:
            data['token_gen_time'] = timedelta(seconds=data['token_gen_time'])
        if 'min_submission_interval' in data and \
                data['min_submission_interval'] is not None:
            data['min_submission_interval'] = \
                timedelta(seconds=data['min_submission_interval'])
        if 'min_user_test_interval' in data and \
                data['min_user_test_interval'] is not None:
            data['min_user_test_interval'] = \
                timedelta(seconds=data['min_user_test_interval'])
        return cls(**data)


class Dataset(Base):
    """Class to store the information about a data set. Not to be used
    directly (import it from SQLAlchemyAll).

    """
    __tablename__ = 'datasets'
    __table_args__ = (
        UniqueConstraint('id', 'task_id', 'description',
                         name='cst_datasets_task_id_description'),
        )

    # Auto increment primary key.
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True)

    # Task id and object owning the testcase.
    task_id = Column(
        Integer,
        ForeignKey(Task.id,
            onupdate="CASCADE", ondelete="CASCADE"),
        nullable=False,
        autoincrement=False)

    task = relationship(
        Task,
        primaryjoin='Task.id==Dataset.task_id',
        foreign_keys=task_id,
        backref=backref('datasets',
                        collection_class=list,
                        cascade="all, delete-orphan",
                        passive_deletes=True))

    description = Column(
        String)

    autojudge = Column(Boolean,
        nullable=False,
        default=False)

    # Time and memory limits for every testcase.
    time_limit = Column(
        Float,
        nullable=True)
    memory_limit = Column(
        Integer,
        nullable=True)

    # Name of the TaskType child class suited for the task.
    task_type = Column(
        String,
        nullable=False)

    # Parameters for the task type class, JSON encoded.
    task_type_parameters = Column(
        String,
        nullable=False)

    # Name of the ScoreType child class suited for the task.
    score_type = Column(
        String,
        nullable=False)

    # Parameters for the score type class, JSON encoded.
    score_type_parameters = Column(
        String,
        nullable=False)

    # Follows the description of the fields automatically added by
    # SQLAlchemy.
    # managers (dict of Manager objects indexed by filename)
    # testcases (list of Testcase objects)

    def export_to_dict(self):
        """Return object data as a dictionary.

        """
        return {'description': self.description,
                'testcases': [testcase.export_to_dict()
                              for testcase in self.testcases],
                'autojudge': self.autojudge,
                'time_limit':           self.time_limit,
                'memory_limit':         self.memory_limit,
                'task_type':            self.task_type,
                'task_type_parameters': self.task_type_parameters,
                'score_type':           self.score_type,
                'score_type_parameters': self.score_type_parameters,
                'managers': [manager.export_to_dict()
                             for manager in self.managers.itervalues()],
            }

    @classmethod
    def import_from_dict(cls, data):
        """Build the object using data from a dictionary.

        """
        data['managers'] = [Manager.import_from_dict(manager_data)
                            for manager_data in data['managers']]
        data['managers'] = dict([(manager.filename, manager)
                                 for manager in data['managers']])
        data['testcases'] = [Testcase.import_from_dict(testcase)
                             for testcase in data['testcases']]
        return cls(**data)


class Testcase(Base):
    """Class to store the information about a testcase. Not to be used
    directly (import it from SQLAlchemyAll).

    """
    __tablename__ = 'task_testcases'
    __table_args__ = (
        UniqueConstraint('dataset_id', 'num',
                         name='cst_task_testcases_dataset_id_num'),
        )

    # Auto increment primary key.
    id = Column(
        Integer,
        primary_key=True)

    # Number of the task for sorting.
    num = Column(
        Integer,
        nullable=False)

    # If the testcase outcome is going to be showed to the user (even
    # without playing a token).
    public = Column(
        Boolean,
        nullable=False,
        default=False)

    # Digests of the input and output files.
    input = Column(
        String,
        nullable=False)
    output = Column(
        String,
        nullable=False)

    # Dataset owning this testcase.
    dataset_id = Column(
        Integer,
        ForeignKey(Dataset.id,
                   onupdate="CASCADE", ondelete="CASCADE"),
        nullable=False,
        index=True)

    # Dataset object owning this testcase.
    dataset = relationship(
        Dataset,
        backref=backref('testcases',
                        collection_class=ordering_list('num'),
                        order_by=[num],
                        cascade="all, delete-orphan",
                        passive_deletes=True))

    def export_to_dict(self):
        """Return object data as a dictionary.

        """
        return {'input':  self.input,
                'output': self.output,
                'public': self.public,
                'num': self.num}

    @classmethod
    def import_from_dict(cls, data):
        """Build the object using data from a dictionary.

        """
        public = data.get('public', None)
        input_ = data.get('input', None)
        output = data.get('output', None)
        res = cls(num=0,
                  public=public if public is not None else True,
                  input=input_ if input_ is not None else "",
                  output=output if output is not None else "")
        res.num = None
        if public is None:
            res.public = None
        if input_ is None:
            res.input = None
        if output is None:
            res.output = None
        return res


class Attachment(Base):
    """Class to store additional files to give to the user together
    with the statement of the task. Not to be used directly (import it
    from SQLAlchemyAll).

    """
    __tablename__ = 'attachments'
    __table_args__ = (
        UniqueConstraint('task_id', 'filename',
                         name='cst_attachments_task_id_filename'),
        )
    # Auto increment primary key.
    id = Column(
        Integer,
        primary_key=True)

    # Filename and digest of the manager.
    filename = Column(
        String,
        nullable=False)
    digest = Column(
        String,
        nullable=False)

    # Task (id and object) owning the attachment.
    task_id = Column(
        Integer,
        ForeignKey(Task.id,
                   onupdate="CASCADE", ondelete="CASCADE"),
        nullable=False,
        index=True)
    task = relationship(
        Task,
        backref=backref('attachments',
                        collection_class=smart_mapped_collection('filename'),
                        cascade="all, delete-orphan",
                        passive_deletes=True))

    def export_to_dict(self):
        """Return object data as a dictionary.

        """
        return {'filename': self.filename,
                'digest':   self.digest}


class Manager(Base):
    """Class to store additional files needed to compile or evaluate a
    submission (e.g., graders). Not to be used directly (import it from
    SQLAlchemyAll).

    """
    __tablename__ = 'managers'
    __table_args__ = (
        UniqueConstraint('dataset_id', 'filename',
                         name='cst_managers_dataset_id_filename'),
        )

    # Auto increment primary key.
    id = Column(
        Integer,
        primary_key=True)

    # Filename and digest of the manager.
    filename = Column(
        String,
        nullable=False)
    digest = Column(
        String,
        nullable=False)

    # Dataset that this evaluation belongs to.
    dataset_id = Column(
        Integer,
        ForeignKey(Dataset.id,
                   onupdate="CASCADE", ondelete="CASCADE"),
        nullable=False,
        index=True)
    dataset = relationship(
        Dataset,
        backref=backref('managers',
                        collection_class=smart_mapped_collection('filename'),
                        cascade="all, delete-orphan",
                        passive_deletes=True))

    def export_to_dict(self):
        """Return object data as a dictionary.

        """
        return {'filename': self.filename,
                'digest':   self.digest}


class SubmissionFormatElement(Base):
    """Class to store the requested files that a submission must
    include. Filenames may include %l to represent an accepted
    language extension. Not to be used directly (import it from
    SQLAlchemyAll).

    """
    __tablename__ = 'submission_format_elements'

    # Auto increment primary key.
    id = Column(
        Integer,
        primary_key=True)

    # Task (id and object) owning the submission format.
    task_id = Column(
        Integer,
        ForeignKey(Task.id,
                   onupdate="CASCADE", ondelete="CASCADE"),
        nullable=False,
        index=True)
    task = relationship(
        Task,
        backref=backref('submission_format',
                        cascade="all, delete-orphan",
                        passive_deletes=True))

    # Format of the given submission file.
    filename = Column(
        String,
        nullable=False)

    def export_to_dict(self):
        """Return object data as a dictionary.

        """
        return {'filename': self.filename}


class Statement(Base):
    """Class to store a translation of the task statement. Not
    to be used directly (import it from SQLAlchemyAll).

    """
    __tablename__ = 'statements'
    __table_args__ = (
        UniqueConstraint('task_id', 'language',
                         name='cst_statements_task_id_language'),
        )

    # Auto increment primary key.
    id = Column(
        Integer,
        primary_key=True)

    # Code for the language the statement is written in.
    # It can be an arbitrary string, but if it's in the form "en" or "en_US"
    # it will be rendered appropriately on the interface (i.e. "English" and
    # "English (United States of America)"). These codes need to be taken from
    # ISO 639-1 and ISO 3166-1 respectively.
    language = Column(
        String,
        nullable=False)

    # Digest of the file.
    digest = Column(
        String,
        nullable=False)

    # Task (id and object) the statement is for.
    task_id = Column(
        Integer,
        ForeignKey(Task.id,
                   onupdate="CASCADE", ondelete="CASCADE"),
        nullable=False,
        index=True)
    task = relationship(
        Task,
        backref=backref('statements',
                        collection_class=smart_mapped_collection('language'),
                        cascade="all, delete-orphan",
                        passive_deletes=True))

    def export_to_dict(self):
        """Return object data as a dictionary.

        """
        return {'language': self.language,
                'digest':   self.digest}
