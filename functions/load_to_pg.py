import psycopg2
import boto3
import pandas as pd
from config.settings import settings

def create_pg_table(connection) -> None:
    cur = connection.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS github_users (
        login TEXT PRIMARY KEY,
        id BIGINT,
        node_id TEXT,
        avatar_url TEXT,
        gravatar_id TEXT,
        url TEXT,
        html_url TEXT,
        followers_url TEXT,
        following_url TEXT,
        gists_url TEXT,
        starred_url TEXT,
        subscriptions_url TEXT,
        organizations_url TEXT,
        repos_url TEXT,
        events_url TEXT,
        received_events_url TEXT,
        type TEXT,
        user_view_type TEXT,
        site_admin BOOLEAN,
        name TEXT,
        company TEXT,
        blog TEXT,
        location TEXT,
        email TEXT,
        hireable BOOLEAN,
        bio TEXT,
        twitter_username TEXT,
        public_repos INT,
        public_gists INT,
        followers INT,
        following INT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );
    """)
    connection.commit()
    cur.close()


def load_to_postgres() -> None:
    print("⬇️  Downloading CSV from S3...")
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=settings.S3_BUCKET, Key=settings.OUTPUT_PATH)
    df = pd.read_csv(obj["Body"])

    print(f"📦 Loaded {len(df)} records from S3 CSV.")

    conn = psycopg2.connect(
        host=settings.POSTGRES_HOST,
        dbname=settings.POSTGRES_DB,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
    )

    create_pg_table(conn)
    cur = conn.cursor()

    print("📤 Inserting into PostgreSQL...")
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO github_users (
                login, id, node_id, avatar_url, gravatar_id, url, html_url,
                followers_url, following_url, gists_url, starred_url, subscriptions_url,
                organizations_url, repos_url, events_url, received_events_url, type,
                user_view_type, site_admin, name, company, blog, location, email,
                hireable, bio, twitter_username, public_repos, public_gists, followers,
                following, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (login) DO NOTHING;
        """, tuple(row.get(col, None) if not pd.isna(row.get(col, None)) else None for col in df.columns))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Data successfully inserted into PostgreSQL.")
