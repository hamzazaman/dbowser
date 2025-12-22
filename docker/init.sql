CREATE TABLE widgets (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE gadgets (
    id SERIAL PRIMARY KEY,
    label TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE widget_events (
    id SERIAL PRIMARY KEY,
    widget_id INTEGER NOT NULL REFERENCES widgets (id),
    event_type TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE long_texts (
    id SERIAL PRIMARY KEY,
    note TEXT NOT NULL
);

INSERT INTO widgets (name, quantity)
VALUES
    ('alpha', 3),
    ('beta', 7),
    ('gamma', 0);

INSERT INTO gadgets (label)
VALUES
    ('flux capacitor'),
    ('optical spanner');

INSERT INTO widget_events (widget_id, event_type)
VALUES
    (1, 'created'),
    (1, 'inspected'),
    (2, 'created');

INSERT INTO long_texts (note)
VALUES
    ('This is a deliberately long cell value used to validate column truncation behavior in the rows view while preserving the full value in the cell detail screen.');
