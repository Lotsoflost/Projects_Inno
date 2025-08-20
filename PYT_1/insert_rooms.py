from connect_db import connector
from psycopg2.extras import execute_values, RealDictCursor
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Union, List, Dict, Optional


# room = {"id": 1, "name": "Room #1"}


def load_json_list(path: Union[str, Path]) -> List[Dict]:
    p = Path(path)
    with p.open("r", encoding= 'utf-8') as f:
        data = json.load(f)
        return data if isinstance(data, list) else [data]


def map_room(j: Dict) -> Dict:
    return {
        "id": int(j["id"]),
        "name": (j.get("name") or "").strip(),
    }


def map_students(j: Dict) -> Dict:
    return {
        "birthday": (j.get("birthday") or "").strip(),
        "id": int(j["id"]),
        "name": (j.get("name") or "").strip(),
        "room": int(j["room"]),
        "sex": (j.get("sex") or "").strip()
    }


def insert_room_bulk(rows: List[Dict]) -> int:
    if not rows:
        return 0
    cols = ["id", "name"]
    values = [[r[c] for c in cols] for r in rows]
    sql = (
        "INSERT INTO base_schema.rooms (id, name) VALUES %s "
        "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"  # OR DO NOTHING
    )
    conn = connector()
    cur = conn.cursor()
    try:
        execute_values(cur, sql, values, template="(%s, %s)")
        conn.commit()
        return len(values)
    finally:
        cur.close()
        conn.close()


def insert_student_bulk(rows: List[Dict]) -> int:
    if not rows:
        return 0
    cols = ["birthday", "id", "name", "room", "sex"]
    values = [[r[c] for c in cols] for r in rows]
    sql = (
        "INSERT INTO base_schema.students (birthday, id, name, room, sex) VALUES %s "
        "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"  
    )
    conn = connector()
    cur = conn.cursor()
    try:
        execute_values (cur, sql, values, template="(%s, %s, %s, %s, %s)")
        conn.commit()
        return len(values)
    finally:
        cur.close()
        conn.close()


def load_rooms(path: Union[str, Path]) -> int:
    raw = load_json_list(path)
    mapped = [map_room(x) for x in raw]
    return insert_room_bulk(mapped)


def load_students(path: Union[str, Path]) -> int:
    raw = load_json_list(path)
    mapped = [map_students(x) for x in raw]
    return insert_student_bulk(mapped)


if __name__ == "__main__":
    inserted_1 = load_rooms("rooms.json")
    inserted_2 = load_students("students.json")
    print({inserted_1: inserted_2, inserted_2: inserted_1})


def run_query_dict(sql: str) -> List[Dict]:
    conn = connector()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            return [dict(r) for r in rows]
    finally:
        conn.close()


def to_json(rows: List[Dict], pretty: bool = True) -> str:
    return json.dumps(rows, ensure_ascii =False, indent = 2 if pretty else None, default=str)


def to_xml(rows: List[Dict], root_tag="rows", item_tag="row") -> str:
    root = ET.Element(root_tag)
    for r in rows:
        item = ET.SubElement(root, item_tag)
        for k, v in r.items():
            el = ET.SubElement(item, k)
            el.text = "" if v is None else str(v)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True).decode("utf-8")        


def export_query(sql: str,
                 fmt: str = "json",
                 outfile: Optional[Union[str,Path]] = None) -> str:
    if fmt not in ("json", "xml"):
            raise ValueError("Format should be either 'json' or 'xml'")

    rows = run_query_dict(sql)
    data = to_json(rows) if fmt == "json" else to_xml(rows)

    if outfile is not None:
        p = Path(outfile)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(data, encoding="utf-8")
    return data

 # Number of students in each room
if __name__ == "__main__":
    sql = """
        SELECT
            COALESCE(s.room, r.id) AS room_number,
            COUNT(s.name)          AS student_cnt
        FROM base_schema.students s
        RIGHT JOIN base_schema.rooms r
               ON s.room = r.id
        GROUP BY s.room, r.id;
         """

    # print(export_query(sql, fmt="json", outfile="out/room_count_members.json"))

    print(export_query(sql, fmt="xml", outfile="out/room_count_members.xml"))

    # Average age for each room (top 5, youngest rooms)
if __name__ == "__main__":
    sql = """
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
         """

    # print(export_query(sql, fmt="json", outfile="out/room_avg_age.json"))

    print(export_query(sql, fmt="xml", outfile="out/room_avg_age.xml"))

    # Minimal age difference within the room (top 5, largest differences)
if __name__ == "__main__":
    sql = """
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
         """

    # print(export_query(sql, fmt="json", outfile="out/room_max_age_diff.json"))

    print(export_query(sql, fmt="xml", outfile="out/room_max_age_diff.xml"))

    # Mixed gender rooms
if __name__ == "__main__":
    sql = """
        SELECT
            room AS room_number,
            TRUE AS mixed_genders
        FROM base_schema.students
        GROUP BY room
        HAVING COUNT(DISTINCT sex) = 2;
         """

    # print(export_query(sql, fmt="json", outfile="out/room_genders.json"))

    print(export_query(sql, fmt="xml", outfile="out/room_genders.xml"))