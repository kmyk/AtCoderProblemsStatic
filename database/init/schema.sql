
CREATE TABLE contests (
    contest_id      VARCHAR(255) PRIMARY KEY,
    contest_name    VARCHAR(255) NOT NULL,
    rated_range     VARCHAR(255) NOT NULL,
    start_at        TIMESTAMP WITH TIME ZONE NOT NULL,
    end_at          TIMESTAMP WITH TIME ZONE NOT NULL,
    inserted_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX ON contests (inserted_at);

CREATE TABLE tasks (
    task_id         VARCHAR(255) PRIMARY KEY,
    task_name       VARCHAR(255) NOT NULL,
    inserted_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX ON tasks (inserted_at);

CREATE TABLE contests_tasks (
    contest_id      VARCHAR(255) REFERENCES contests,
    task_id         VARCHAR(255) REFERENCES tasks,
    alphabet        VARCHAR(255),
    inserted_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (contest_id, task_id)
);
CREATE INDEX ON contests (inserted_at);

CREATE TABLE users (
    user_id         VARCHAR(255) PRIMARY KEY,
    inserted_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE renamed (
    user_id_from    VARCHAR(255) NOT NULL UNIQUE,
    user_id_to      VARCHAR(255) NOT NULL UNIQUE,
    inserted_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id_from, user_id_to)
);

CREATE TABLE submissions (
    submission_id   BIGINT PRIMARY KEY,
    contest_id      VARCHAR(255) REFERENCES contests,
    task_id         VARCHAR(255) REFERENCES tasks,
    user_id         VARCHAR(255) REFERENCES users,
    submitted_at    TIMESTAMP WITH TIME ZONE NOT NULL,
    language_name   VARCHAR(255) NOT NULL,
    score           DOUBLE PRECISION NOT NULL,
    code_size       INT NOT NULL,
    status          VARCHAR(255) NOT NULL,
    execution_time  INT NULL,
    memory_consumed INT NULL,
    inserted_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (contest_id, task_id) REFERENCES contests_tasks
);
CREATE INDEX ON submissions (user_id, inserted_at);
CREATE INDEX ON submissions (contest_id, submission_id);
