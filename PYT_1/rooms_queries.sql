-- =====================================================
-- SCHEMA SETUP
-- =====================================================

-- Create tables
CREATE TABLE base_schema.rooms (
    id   INTEGER PRIMARY KEY,
    name VARCHAR
);

ALTER TABLE base_schema.rooms
    OWNER TO postgres;

CREATE TABLE base_schema.students (
    id       INTEGER PRIMARY KEY,
    name     VARCHAR(100),
    birthday TIMESTAMP,
    room     BIGINT,
    sex      VARCHAR(10)
);

ALTER TABLE base_schema.students
    OWNER TO postgres;

-- =====================================================
-- MAINTENANCE QUERIES
-- =====================================================

-- Truncate tables
TRUNCATE base_schema.rooms;
TRUNCATE base_schema.students;

-- Check contents
SELECT * FROM base_schema.rooms;
SELECT * FROM base_schema.students;

-- =====================================================
-- ANALYTICAL QUERIES
-- =====================================================

-- 1. Number of students in each room
-- Hash join handles this efficiently, no index required
SELECT 
    COALESCE(s.room, r.id) AS room_number,
    COUNT(s.name)          AS student_cnt
FROM base_schema.students s
RIGHT JOIN base_schema.rooms r
       ON s.room = r.id
GROUP BY s.room, r.id;

-- 2. Average age for each room (top 5, youngest rooms)
WITH cte_student_ages_in_days AS (
    SELECT 
        room AS room_number,
        (CURRENT_DATE - birthday::date) AS age_in_days
    FROM base_schema.students
),
cte_average_age_in_days AS (
    SELECT
        room_number,
        AVG(age_in_days) AS average_age_in_days
    FROM cte_student_ages_in_days
    GROUP BY room_number
)
SELECT 
    room_number,
    ((average_age_in_days / 365.25)::INTEGER) || ' years ' ||
    ((average_age_in_days % 365.25 / 30.44)::INTEGER) || ' mons ' ||
    ((average_age_in_days % 365.25 % 30.44)::INTEGER) || ' days' AS average_age
FROM cte_average_age_in_days
ORDER BY average_age_in_days ASC
LIMIT 5;

-- 3. Minimal age difference within the room (top 5, largest differences)
WITH cte_age_diff AS (
    SELECT 
        room AS room_number,
        MAX(birthday::date) - MIN(birthday::date) AS age_diff
    FROM base_schema.students
    GROUP BY room
)
SELECT 
    room_number,
    ((age_diff / 365.25)::INTEGER) || ' years ' ||
    ((age_diff % 365.25 / 30.44)::INTEGER) || ' mons ' ||
    ((age_diff % 365.25 % 30.44)::INTEGER) || ' days' AS age_diff_fmt
FROM cte_age_diff
ORDER BY age_diff DESC
LIMIT 5;

-- 4. Mixed gender rooms
SELECT 
    room AS room_number, 
    TRUE AS mixed_genders
FROM base_schema.students
GROUP BY room
HAVING COUNT(DISTINCT sex) = 2;

-- =====================================================
-- INDEXES
-- =====================================================

-- For faster aggregations by room
CREATE INDEX idx_students_room 
    ON base_schema.students(room);

-- For fast min/max birthday lookup per room
CREATE INDEX CONCURRENTLY idx_students_room_birthday_asc
    ON base_schema.students(room, birthday ASC);

CREATE INDEX CONCURRENTLY idx_students_room_birthday_desc
    ON base_schema.students(room, birthday DESC);

-- =====================================================
-- OPTIMIZED MIN/MAX QUERY (uses indexes)
-- =====================================================
EXPLAIN
WITH min_b AS (
    SELECT DISTINCT ON (room) 
           room, birthday::date AS min_b
    FROM base_schema.students
    ORDER BY room, birthday ASC
),
max_b AS (
    SELECT DISTINCT ON (room) 
           room, birthday::date AS max_b
    FROM base_schema.students
    ORDER BY room, birthday DESC
)
SELECT 
    m.room,
    (x.max_b - m.min_b) AS age_diff_days
FROM min_b m
JOIN max_b x USING (room)
ORDER BY age_diff_days DESC
LIMIT 5;

-- now this query works longer but it will be useful if amount of data enlarges enormously