CREATE TABLE widgets (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 0
);

INSERT INTO widgets (name, quantity)
VALUES
    ('alpha', 3),
    ('beta', 7),
    ('gamma', 0);
