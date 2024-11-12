CREATE TABLE pokemon (
    id SERIAL, 
    name TEXT PRIMARY KEY,
    type_1 TEXT,
    type_2 TEXT,
    total INTEGER,
    hp INTEGER,
    attack INTEGER,
    defense INTEGER,
    sp_atk INTEGER,
    sp_def INTEGER,
    speed INTEGER,
    generation INTEGER,
    legendary BOOLEAN
);

COPY pokemon (id, name, type_1, type_2, total, hp, attack, defense, sp_atk, sp_def, speed, generation, legendary)
FROM '/tmp/Pokemon.csv' DELIMITER ',' CSV HEADER;
