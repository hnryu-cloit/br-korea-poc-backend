import psycopg
from _runner import run_main


def check_vector():
    url = "postgresql://postgres:postgres@localhost:5435/br_korea_poc"
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM pg_extension WHERE extname = 'vector';")
            result = cur.fetchone()
            print(f"Vector extension: {result}")


if __name__ == "__main__":
    run_main(check_vector)