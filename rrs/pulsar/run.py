"""Pulsar pipeline (one search): download themes XLS + capture session token →
parse (themes + topics) → store into pulsar_* tables → fetch each theme's top posts
via GraphQL → store posts.

Run with: python -m rrs.pulsar.run
"""

from rrs.pulsar import store
from rrs.pulsar.download_themes import download_themes_xlsx
from rrs.pulsar.parse_themes import parse_themes_xlsx
from rrs.pulsar.posts_client import PulsarPostsClient
from rrs.pulsar.settings import PulsarSettings


def run() -> None:
    s = PulsarSettings.from_env()

    # 1. Drive the dashboard: download the themes XLS + capture the session token.
    dl = download_themes_xlsx(
        email=s.email,
        password=s.password,
        folder_id=s.folder_id,
        search_id=s.search_id,
        output_path=s.themes_xlsx,
        base_url=s.base_url,
        headless=s.headless,
    )

    # 2. Parse themes + topic breakdown.
    export = parse_themes_xlsx(dl.xlsx_path)
    print(f"  parsed {len(export.themes)} theme(s) from '{export.search_name}' "
          f"({export.date_from} → {export.date_to})")

    posts_client = PulsarPostsClient(dl.authorization, dl.team) if dl.authorization else None
    if posts_client is None:
        print("  WARNING: no session token captured — storing themes/topics only, posts skipped.")

    # 3. Store searches/themes/topics; 4. fetch + store each theme's posts.
    total_posts = 0
    with store.connect() as conn:
        store.upsert_search(conn, s.search_id, export.search_name, s.folder_id, s.base_url)
        for theme in export.themes:
            theme_id = store.insert_theme(conn, s.search_id, theme, export.date_from, export.date_to)
            if posts_client and theme.topics:
                posts = posts_client.fetch_theme_posts(
                    s.search_id,
                    [t.label for t in theme.topics],
                    export.date_from,
                    export.date_to,
                    limit=s.top_n,
                )
                total_posts += store.upsert_posts(conn, s.search_id, theme_id, posts)
        conn.commit()

    print(f"  stored {len(export.themes)} theme(s), {total_posts} post(s) for search {s.search_id}.")


if __name__ == "__main__":
    run()
