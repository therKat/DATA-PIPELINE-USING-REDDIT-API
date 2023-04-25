import json
import praw
import psycopg2

class RedditPipeline:
    def __init__(self, config):
        self.reddit = praw.Reddit(
            client_id=config['reddit']['client_id'],
            client_secret=config['reddit']['client_secret'],
            username=config['reddit']['username'],
            password=config['reddit']['password'],
            user_agent=config['reddit']['user_agent']
        )
        self.subreddit = None

    def connect_to_subreddit(self, subreddit_name):
        self.subreddit = self.reddit.subreddit(subreddit_name)

    def fetch_posts(self, limit=10):
        posts = []
        for submission in self.subreddit.hot(limit=limit):
            post = {
                "id": submission.id,
                "title": submission.title,
                "score": submission.score,
                "url": submission.url
            }
            posts.append(post)
        return posts

class PostgresPipeline:
    def __init__(self, config):
        self.conn = psycopg2.connect(
            host=config['postgres']['host'],
            database=config['postgres']['database'],
            user=config['postgres']['user'],
            password=config['postgres']['password']
        )
        self.cursor = self.conn.cursor()

    def create_table(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS subreddit (
                id TEXT PRIMARY KEY,
                title TEXT,
                score INTEGER,
                url TEXT
            )
        """)

    def save_to_database(self, posts):
        try:
            for post in posts:
                data = (post["id"], post["title"], post["score"], post["url"])
                self.cursor.execute("""
                    INSERT INTO subreddit (id, title, score, url)
                    SELECT %s, %s, %s, %s
                    WHERE NOT EXISTS (
                      SELECT 1
                      FROM subreddit
                      WHERE id = %s
                    )
                """, (*data, post["id"]))
            self.conn.commit()
        except:
            self.conn.rollback()
            raise

    def close(self):
        self.cursor.close

def main():
    # tải tập tin cấu hình
    with open('config.json') as f:
        config = json.load(f)

    # create Reddit pipeline
    reddit_pipeline = RedditPipeline(config)

    # connect to a subreddit
    reddit_pipeline.connect_to_subreddit("redditdev")

    # chỉ đến posts
    posts = reddit_pipeline.fetch_posts(limit=10)

    # tạo Postgres pipeline
    postgres_pipeline = PostgresPipeline(config)

    # tạo bảng
    postgres_pipeline.create_table()

    # lưu vào database
    postgres_pipeline.save_to_database(posts)

    # đóng kết nối
    postgres_pipeline.close()


if __name__ == '__main__':
    main()
