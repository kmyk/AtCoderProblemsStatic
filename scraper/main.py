# Python Version: 3.x
import contextlib
import datetime
import json
import os
import random
import time
import traceback
from logging import getLogger, StreamHandler, DEBUG
from typing import *

import psycopg2
import requests
from onlinejudge.service.atcoder import AtCoderService, AtCoderContest, AtCoderProblem, AtCoderSubmission


logger = getLogger(__name__)
handler = StreamHandler()
handler.setLevel(DEBUG)
logger.setLevel(DEBUG)
logger.addHandler(handler)


@contextlib.contextmanager
def db():
    config = {
        "host": os.environ["POSTGRES_HOST"],
        "user": os.environ["POSTGRES_USER"],
    }
    dsn = ' '.join(map('='.join, config.items()))
    with psycopg2.connect(dsn) as conn:
        conn.autocommit = True
        yield conn


def scrape_contests(*, session, conn):
    service = AtCoderService()
    for contest in service.iterate_contests(session=session):
        time.sleep(1)

        with conn.cursor() as cur:
            values = (
                contest.contest_id,
                contest.get_name(lang='ja', session=session),
                contest.get_rated_range(session=session),
                contest.get_start_time(session=session),
                contest.get_start_time(session=session) + contest.get_duration(session=session),
            )
            try:
                cur.execute("""
                    INSERT INTO contests (contest_id, contest_name, rated_range, start_at, end_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, values)
            except psycopg2.errors.UniqueViolation:
                pass
            else:
                logger.debug('INSERT INTO contests: %s', contest.get_url())

        scrape_tasks(contest, session=session, conn=conn)

def scrape_tasks(contest: AtCoderContest, *, session, conn):
    try:
        problems = contest.list_problems(session=session)
    except requests.exceptions.HTTPError:
        traceback.print_exc()
        return

    for problem in problems:
        time.sleep(1)

        with conn.cursor() as cur:
            values = (
                problem.problem_id,
                problem.get_name(session=session),
            )
            try:
                cur.execute("""
                    INSERT INTO tasks (task_id, task_name)
                    VALUES (%s, %s)
                """, values)
            except psycopg2.errors.UniqueViolation:
                pass
            else:
                logger.debug('INSERT INTO tasks: %s', problem.get_url())

        with conn.cursor() as cur:
            values = (
                problem.contest_id,
                problem.problem_id,
                problem.get_alphabet(),
            )
            try:
                cur.execute("""
                    INSERT INTO contests_tasks (contest_id, task_id, alphabet)
                    VALUES (%s, %s, %s)
                """, values)
            except psycopg2.errors.UniqueViolation:
                pass
            else:
                logger.debug('INSERT INTO contests_tasks: %s', problem.get_url())


def select_contests(*, conn) -> List[AtCoderContest]:

    with conn.cursor() as cur:
        cur.execute("""
            SELECT contest_id FROM contests
        """)
        return [AtCoderContest(contest_id) for contest_id, in cur.fetchall()]


SUBMISSIONS_IN_PAGE = 20


def get_next_page(contest: AtCoderContest, *, conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT count(submission_id) FROM submissions WHERE contest_id = %s
        """, (contest.contest_id,))
        count, = cur.fetchone()
        return count // SUBMISSIONS_IN_PAGE + 1


def insert_submission(submission: AtCoderSubmission, *, session, conn):
    user_id = submission.get_user_id(session=session)
    with conn.cursor() as cur:
        try:
            cur.execute("""
                INSERT INTO users (user_id) VALUES (%s)
            """, (user_id,))
        except psycopg2.errors.UniqueViolation:
            pass
        else:
            logger.debug('INSERT INTO users: https://atcoder.jp/users/%s', user_id)

    with conn.cursor() as cur:
        values = {
            "submission_id": submission.submission_id,
            "contest_id": submission.get_problem(session=session).contest_id,
            "task_id": submission.get_problem(session=session).problem_id,
            "user_id": user_id,
            "submitted_at": submission.get_submission_time(session=session),
            "language_name": submission.get_language_name(session=session),
            "score": submission.get_score(session=session),
            "code_size": submission.get_code_size(session=session),
            "status": submission.get_status(session=session),
            "execution_time": submission.get_exec_time_msec(session=session),
            "memory_consumed": submission.get_memory_byte(session=session) and submission.get_memory_byte(session=session) // 1000,
        }
        keys = list(values.keys())
        try:
            sql = """
                INSERT INTO submissions ({}) VALUES ({})
            """.format(", ".join(keys), ", ".join(["%s"] * len(keys)))
            cur.execute(sql, tuple(values[key] for key in keys))
        except psycopg2.errors.UniqueViolation:
            pass
        else:
            logger.debug('INSERT INTO submissions: %s', submission.get_url())


def run(*, session, conn):
    if random.random() < 0.01:
        scrape_contests(session=session, conn=conn)

    contests = select_contests(conn=conn)
    random.shuffle(contests)
    for contest in contests:
        time.sleep(1)

        page = get_next_page(contest, conn=conn)
        # for submission in contest.iterate_submissions_where(session=session, pages=itertools.count(page)):
        for submission in contest.iterate_submissions_where(session=session):
            time.sleep(1)
            logger.debug('INSERT INTO submissions: %s', submission.get_url())

            insert_submission(submission, session=session, conn=conn)


def main():
    while True:
        try:
            with db() as conn:
                with requests.Session() as session:
                    while True:
                        run(session=session, conn=conn)
                        time.sleep(10)
        except:
            traceback.print_exc()
            time.sleep(60)

if __name__ == "__main__":
    main()
