# Python Version: 3.x
import contextlib
import itertools
import json
import os
import time
import traceback
from logging import DEBUG, StreamHandler, getLogger
from typing import *

import psycopg2
import requests

from onlinejudge.service.atcoder import AtCoderContest, AtCoderProblem, AtCoderService, AtCoderSubmission

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


def scrape_contests(*, session: requests.Session, conn: psycopg2.extensions.connection) -> None:
    service = AtCoderService()
    for contest in service.iterate_contests(session=session):
        logger.debug("scrape_contest(): %s", contest.get_url())

        with conn.cursor() as cur:
            values = (
                contest.contest_id,
                contest.get_name(lang='ja', session=session),
                contest.get_rated_range(session=session),
                contest.get_start_time(session=session),
                contest.get_start_time(session=session) + contest.get_duration(session=session),
            )
            cur.execute("""
                INSERT INTO contests (contest_id, contest_name, rated_range, start_at, end_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, values)
            if cur.rowcount:
                logger.debug('INSERT INTO contests: %s', contest.get_url())

        scrape_tasks(contest, session=session, conn=conn)


def scrape_tasks(contest: AtCoderContest, *, session: requests.Session, conn: psycopg2.extensions.connection) -> None:
    time.sleep(1)
    try:
        problems = contest.list_problems(session=session)
    except requests.exceptions.HTTPError:
        # This happens when the contest is running yet.
        traceback.print_exc()
        return

    for problem in problems:

        with conn.cursor() as cur:
            values1 = (
                problem.problem_id,
                problem.get_name(session=session),
            )
            cur.execute("""
                INSERT INTO tasks (task_id, task_name)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, values1)
            if cur.rowcount:
                logger.debug('INSERT INTO tasks: %s', problem.get_url())

        with conn.cursor() as cur:
            values2 = (
                problem.contest_id,
                problem.problem_id,
                problem.get_alphabet(session=session),
            )
            cur.execute("""
                INSERT INTO contests_tasks (contest_id, task_id, alphabet)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
            """, values2)
            if cur.rowcount:
                logger.debug('INSERT INTO contests_tasks: %s', problem.get_url())


def select_contests(*, conn: psycopg2.extensions.connection) -> List[AtCoderContest]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT contest_id FROM contests
        """)
        return [AtCoderContest(contest_id) for contest_id, in cur.fetchall()]


SUBMISSIONS_IN_PAGE = 20


def get_next_page(contest: AtCoderContest, *, conn: psycopg2.extensions.connection) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT count(submission_id) FROM submissions WHERE contest_id = %s
        """, (contest.contest_id, ))
        count, = cur.fetchone()
        return count // SUBMISSIONS_IN_PAGE + 1


def insert_submission(submission: AtCoderSubmission, *, session: requests.Session, conn: psycopg2.extensions.connection) -> bool:
    user_id = submission.get_user_id(session=session)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (user_id) VALUES (%s)
            ON CONFLICT DO NOTHING
        """, (user_id, ))
        if cur.rowcount:
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
        sql = """
            INSERT INTO submissions ({}) VALUES ({})
            ON CONFLICT DO NOTHING
        """.format(", ".join(keys), ", ".join(["%s"] * len(keys)))
        cur.execute(sql, tuple(values[key] for key in keys))
        if cur.rowcount:
            logger.debug("INSERT INTO submissions: %s", submission.get_url())
        return cur.rowcount != 0


def is_user_updated(user_id: int, *, session: requests.Session) -> bool:
    url = "https://atcoder.jp/users/{}".format(user_id)
    resp = session.get(url)
    return resp.status_code == 404


def report_updated_user(user_id: int, *, session: requests.Session, conn: psycopg2.extensions.connection) -> None:
    logger.debug("report_updated_user(): %s", user_id)
    assert is_user_updated(user_id, session=session)

    # check whether it is unregistered or just renamed
    with conn.cursor() as cur:
        cur.execute("""
            SELECT contest_id, submission_id FROM submissions WHERE user_id = %s
        """, (user_id, ))
        contest_id, submission_id, = cur.fetchone()
        submission = AtCoderSubmission(contest_id, submission_id)
    try:
        renamed = submission.get_user_id(session=session)
    except requests.exceptions.HTTPError:
        renamed = None

    # update the local database
    if renamed is None:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM submissions WHERE user_id = %s
            """, (user_id, ))
            logger.debug("DELETE FROM submissions: user_id = %s", user_id)
    else:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO renamed (user_id_from, user_id_to) VALUES (%s, %s)
            """, (user_id, renamed))
            logger.debug("INSERT INTO renamed: %s -> %s", user_id, renamed)


def get_expected_submissions(contest: AtCoderContest, page: int, *, limit: Optional[int] = None, conn: psycopg2.extensions.connection) -> List[AtCoderSubmission]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT submission_id FROM submissions WHERE contest_id = %s
            ORDER BY submission_id
            OFFSET %s """ + ("LIMIT %s" if limit is not None else "") + """
        """, (contest.contest_id, (page - 1) * SUBMISSIONS_IN_PAGE, *([limit] if limit is not None else [])))
        rows = cur.fetchall()
    return [AtCoderSubmission(contest.contest_id, submission_id) for submission_id, in rows]


def is_submissions_page_broken(contest: AtCoderContest, page: int, *, session: requests.Session, conn: psycopg2.extensions.connection) -> bool:
    time.sleep(1)
    is_broken = False
    a = (get_expected_submissions(contest, page, limit=SUBMISSIONS_IN_PAGE, conn=conn) + [None] * SUBMISSIONS_IN_PAGE)[:SUBMISSIONS_IN_PAGE]
    b1 = contest.iterate_submissions_where(order='created', desc=False, pages=itertools.count(page), session=session)
    b = sorted(list(itertools.islice(b1, 0, 20)), key=lambda submission: submission.submission_id)
    for expected, submission in zip(a, b):
        insert_submission(submission, session=session, conn=conn)
        if expected is None:
            pass
        elif expected.submission_id > submission.submission_id:
            is_broken = True  # a lack of rows
        elif expected.submission_id < submission.submission_id:
            is_broken = True  # remote rows are deleted
    return is_broken


def scrape_lost_submissions_for_contest(contest: AtCoderContest, *, session: requests.Session, conn: psycopg2.extensions.connection) -> bool:
    logger.debug("scrape_lost_submissions_for_submission(): %s", contest.get_url())

    # binary search
    pred = lambda page: is_submissions_page_broken(contest, page, session=session, conn=conn)
    l, r = 0, get_next_page(contest, conn=conn) + 1
    if not pred(r - 1):
        return False  # heuristic
    while r - l > 1:
        m = (l + r) // 2
        if pred(m):
            r = m
        else:
            l = m
    page = r

    # do recovery
    logger.debug("%s recobery from page %d", contest.get_url(), page)
    expected = get_expected_submissions(contest, page, conn=conn)
    inserted = SUBMISSIONS_IN_PAGE
    for i, submission in enumerate(contest.iterate_submissions_where(order='created', desc=False, pages=itertools.count(page), session=session)):
        if i < len(expected):
            if expected[i].submission_id < submission.submission_id:
                # remote rows are deleted
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT user_id FROM submissions WHERE submission_id = %s
                    """, (expected[i].submission_id, ))
                    user_id, = cur.fetchone()
                if is_user_updated(user_id, session=session):
                    report_updated_user(user_id, session=session, conn=conn)
                    break
        if insert_submission(submission, session=session, conn=conn):
            inserted = min(2 * SUBMISSIONS_IN_PAGE, inserted + 1)
        else:
            inserted = max(0, inserted - 1)
        if (i + 1) % SUBMISSIONS_IN_PAGE == 0 and not inserted:
            break
    return True


def scrape_lost_submissions(*, session: requests.Session, conn: psycopg2.extensions.connection) -> None:
    for contest in select_contests(conn=conn):
        try:
            while scrape_lost_submissions_for_contest(contest, session=session, conn=conn):
                pass
        except:
            traceback.print_exc()


def scrape_submissions(*, session: requests.Session, conn: psycopg2.extensions.connection) -> None:
    for contest in select_contests(conn=conn):
        logger.debug("scrape_submissions(): %s", contest.get_url())
        try:
            page = get_next_page(contest, conn=conn)
            for submission in contest.iterate_submissions_where(order='created', desc=False, pages=itertools.count(page), session=session):
                time.sleep(1 / SUBMISSIONS_IN_PAGE)
                insert_submission(submission, session=session, conn=conn)
        except:
            traceback.print_exc()


def main() -> None:
    with db() as conn:
        with requests.Session() as session:
            scrape_contests(session=session, conn=conn)
            scrape_lost_submissions(session=session, conn=conn)
            scrape_submissions(session=session, conn=conn)


if __name__ == "__main__":
    main()
