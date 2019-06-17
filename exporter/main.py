# Python Version: 3.x
import contextlib
import datetime
import json
import os
import pathlib
import shutil
import tempfile
from logging import DEBUG, StreamHandler, getLogger
from typing import *

import psycopg2
import psycopg2.extras

EXPORT_DIR = pathlib.Path(os.environ.get("EXPORT_DIR", os.path.curdir)).resolve()

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


def export_contests(*, conn: psycopg2.extensions.connection) -> None:
    path = EXPORT_DIR / "contests.json"
    logger.info("write: %s", path)
    with open(path, "w") as fh:

        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT contest_id, contest_name, rated_range, start_at, end_at
                FROM contests
                ORDER BY contest_id
            """)
            for i, row in enumerate(cur.fetchall()):
                if i == 0:
                    fh.write("[")
                else:
                    fh.write(",")
                data = {
                    "id": row["contest_id"],
                    "title": row["contest_name"],
                    "start_epoch_second": int(row["start_at"].timestamp()),
                    "duration_second": int((row["end_at"] - row["start_at"]).total_seconds()),
                    "rate_change": row["rated_range"],
                }
                fh.write(json.dumps(data, separators=(',', ':'), sort_keys=True, ensure_ascii=False) + "\n")
        fh.write("]\n")


def export_tasks(*, conn: psycopg2.extensions.connection) -> None:
    path = EXPORT_DIR / "problems.json"
    logger.info("write: %s", path)
    with open(path, "w") as fh:

        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT contest_id, task_id, alphabet, task_name
                FROM contests_tasks
                INNER JOIN tasks USING (task_id)
                ORDER BY (contest_id, task_id)
            """)
            for i, row in enumerate(cur.fetchall()):
                if i == 0:
                    fh.write("[")
                else:
                    fh.write(",")
                data = {
                    "id": row["task_id"],
                    "contest_id": row["contest_id"],
                    "title": row["alphabet"] + ". " + row["task_name"],
                }
                fh.write(json.dumps(data, separators=(',', ':'), sort_keys=True, ensure_ascii=False) + "\n")
        fh.write("]\n")


def export_contests_tasks(*, conn: psycopg2.extensions.connection) -> None:
    path = EXPORT_DIR / "contest-problem.json"
    logger.info("write: %s", path)
    with open(path, "w") as fh:

        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT contest_id, task_id
                FROM contests_tasks
                ORDER BY (contest_id, task_id)
            """)
            for i, row in enumerate(cur.fetchall()):
                if i == 0:
                    fh.write("[")
                else:
                    fh.write(",")
                data = {
                    "contest_id": row["contest_id"],
                    "problem_id": row["task_id"],
                }
                fh.write(json.dumps(data, separators=(',', ':'), sort_keys=True) + "\n")
        fh.write("]\n")


def iterate_aliases_for_user(user_id: str, *, conn: psycopg2.extensions.connection) -> Iterator[str]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT user_id_to FROM renamed WHERE user_id_from = %s
        """, (user_id, ))
        if cur.fetchone() is not None:
            return
    while True:
        yield user_id
        with conn.cursor() as cur:
            cur.execute("""
                SELECT user_id_from FROM renamed WHERE user_id_to = %s
            """, (user_id, ))
            row = cur.fetchone()
        if row is None:
            break
        else:
            user_id, = row


def export_submissions_for_user(user_id: str, *, conn: psycopg2.extensions.connection) -> None:
    path = EXPORT_DIR / "results" / user_id[:2].lower() / (user_id + ".json")

    aliases = list(iterate_aliases_for_user(user_id, conn=conn))
    if not aliases:
        if path.exists():
            logger.info("unlink: %s", path)
            path.unlink()
        return

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        sql = """
            SELECT inserted_at
            FROM submissions
            WHERE """ + " OR ".join(["user_id = %s"] * len(aliases)) + """
            ORDER BY inserted_at DESC
            LIMIT 1
        """
        cur.execute(sql, aliases)
        inserted_at, = cur.fetchone() or (datetime.datetime.now(), )

    if path.exists() and inserted_at.timestamp() < path.stat().st_mtime:
        return
    path.parent.mkdir(parents=True, exist_ok=True)

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        sql = """
            SELECT submission_id, contest_id, task_id, submitted_at, language_name, score, code_size, status, execution_time
            FROM submissions
            WHERE """ + " OR ".join(["user_id = %s"] * len(aliases)) + """
            ORDER BY submission_id
        """
        cur.execute(sql, aliases)
        rows = cur.fetchall()

    if not rows:
        if path.exists():
            logger.info("unlink: %s", path)
            path.unlink()
    else:
        with tempfile.NamedTemporaryFile() as fh:
            temppath = pathlib.Path(fh.name)
        try:
            with open(temppath, "w") as fh:
                for i, row in enumerate(rows):
                    if i == 0:
                        fh.write("[")
                    else:
                        fh.write(",")
                    data = {
                        "id": row["submission_id"],
                        "epoch_second": int(row["submitted_at"].timestamp()),
                        "problem_id": row["task_id"],
                        "contest_id": row["contest_id"],
                        "user_id": user_id,
                        "language": row["language_name"],
                        "point": row["score"],
                        "length": row["code_size"],
                        "result": row["status"],
                        "execution_time": "" if row["execution_time"] is None else row["execution_time"],
                    }
                    fh.write(json.dumps(data, separators=(',', ':'), sort_keys=True, ensure_ascii=False) + "\n")
                fh.write("]")
            logger.info("write: %s", path)
            shutil.copyfile(temppath, path)  # almost atomic write, over differennt filesystems
        finally:
            if temppath.exists():
                temppath.unlink()


def export_submissions(*, conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT user_id FROM users
        """)
        for user_id, in cur.fetchall():
            export_submissions_for_user(user_id, conn=conn)


def export() -> None:
    with db() as conn:
        export_contests(conn=conn)
        export_tasks(conn=conn)
        export_contests_tasks(conn=conn)
        export_submissions(conn=conn)


def main() -> None:
    export()


if __name__ == "__main__":
    main()
