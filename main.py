import argparse
import socket
from datetime import datetime, timedelta
from wsgiref.simple_server import make_server

import falcon
import psycopg2
import redis
import requests
from pydantic import BaseSettings


class Settings(BaseSettings):
    postgres_uri_polaris: str
    postgres_uri_vela: str
    redis_url: str


settings = Settings()


def get_yesterdays_asos_users():
    today = datetime.today().date()
    yesterday = str(today - timedelta(days=1))
    sql = f"""
    SELECT count(1)
        FROM account_holder
        WHERE created_at BETWEEN
            '{yesterday}' AND '{today}'
        AND retailer_id = 2
        AND status = 'ACTIVE';
    """
    with psycopg2.connect(settings.postgres_uri_polaris) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchone()[0]


def get_yesterdays_asos_transactions():
    today = datetime.today().date()
    yesterday = str(today - timedelta(days=1))
    sql = f"""
    SELECT count(1)
        FROM processed_transaction
        WHERE created_at BETWEEN
            '{yesterday}' AND '{today}'
        AND retailer_id = 2;
    """
    with psycopg2.connect(settings.postgres_uri_vela) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchone()[0]


def get_all_asos_users_since_27th():
    sql = """
    SELECT count(1)
        FROM account_holder
        WHERE created_at >= '2022-04-27'
        AND retailer_id = 2
        AND status = 'ACTIVE';
    """
    with psycopg2.connect(settings.postgres_uri_polaris) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchone()[0]


def get_all_asos_transactions_since_27th():
    sql = """
    SELECT count(1)
        FROM processed_transaction
            WHERE created_at >= '2022-04-27'
            AND retailer_id = 2;
    """
    with psycopg2.connect(settings.postgres_uri_vela) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchone()[0]


def teams_notification():
    webhook_url = "https://hellobink.webhook.office.com/webhookb2/66f08760-f657-42af-bf88-4f7e4c009af1@a6e2367a-92ea-4e5a-b565-723830bcc095/IncomingWebhook/928e4fefbd904522851850dba09008be/48aca6b1-4d56-4a15-bc92-8aa9d97300df"  # noqa
    json = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "000000",
        "summary": "ASOS User Stats",
        "Sections": [
            {
                "activityTitle": "BPL ASOS Accounts since yesterday",
                "facts": [
                    {"name": "New Users", "value": get_yesterdays_asos_users()},
                    {"name": "New Transactions", "value": get_yesterdays_asos_transactions()},
                ],
                "markdown": False,
            },
            {
                "activityTitle": "BPL ASOS Accounts since launch",
                "facts": [
                    {"name": "Total Users", "value": get_all_asos_users_since_27th()},
                    {"name": "Total Transactions", "value": get_all_asos_transactions_since_27th()},
                ],
                "markdown": False,
            },
        ],
        "potentialAction": [
            {
                "@type": "OpenUri",
                "name": "View Live Stats",
                "targets": [
                    {
                        "os": "default",
                        "uri": "https://bpl.gb.bink.com/hypercounter?auth=78f71dcb-046e-4ee5-b4f8-5d28c787301e",
                    }
                ],
            }
        ],
    }
    return requests.post(webhook_url, json=json)


def is_leader():
    r = redis.Redis.from_url(settings.redis_url)
    lock_key = "bpl-hypercounter"
    hostname = socket.gethostname()
    is_leader = False

    with r.pipeline() as pipe:
        try:
            pipe.watch(lock_key)
            leader_host = pipe.get(lock_key)
            if leader_host in (hostname.encode(), None):
                pipe.multi()
                pipe.setex(lock_key, 60, hostname)
                pipe.execute()
                is_leader = True
        except redis.WatchError:
            pass
    return is_leader


class Home:
    def on_get(self, req, resp):
        print(req.query_string)
        if req.query_string == "auth=78f71dcb-046e-4ee5-b4f8-5d28c787301e":
            resp.status = falcon.HTTP_200
            resp.content_type = falcon.MEDIA_HTML
            resp.text = f"""
            <html>
            <head>
                <link href='http://fonts.googleapis.com/css?family=Poppins' rel='stylesheet' type='text/css'>

                <style>
                    body {{
                        background-color: #3c757b;
                        font-family: 'Poppins', 'Montserrat', sans-serif;
                        color: white;
                        text-align: center;
                    }}

                    .headerImage {{
                        padding: 50px;
                        max-height: 250px;
                    }}

                    .statTitle {{
                        font-size: 18px;
                        font-weight: 400;
                        margin-bottom: 50px;
                    }}

                    .statValue {{
                        font-size: 52px;
                        font-weight: 700;
                        margin-bottom: 5px;
                    }}

                </style>

            </head>
            <body>
                <img class="headerImage" src="https://bpl.gb.bink.com/content/hypercounter/asos.png" />

                <div class="statValue">{get_all_asos_users_since_27th()}</div>
                <div class="statTitle">Total Users</div>

                <div class="statValue">{get_all_asos_transactions_since_27th()}</div>
                <div class="statTitle">Total Transactions</div>

                <div class="statValue">{get_yesterdays_asos_users()}</div>
                <div class="statTitle">Yesterdays New Users</div>

                <div class="statValue">{get_yesterdays_asos_transactions()}</div>
                <div class="statTitle">Yesterdays New Transactions</div>
            </body>
            </html>
            """
        else:
            resp.status = falcon.HTTP_401
            resp.content_type = falcon.MEDIA_TEXT
            resp.text = """
            Access Denied
            """


class Healthz:
    def on_get(self, req, resp):
        resp.status = falcon.HTTP_204


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", action="store_true")
    parser.add_argument("--printer", action="store_true")
    parser.add_argument("--teams", action="store_true")
    args = parser.parse_args()
    if args.server:
        app = falcon.App()
        app.add_route("/healthz", Healthz())
        app.add_route("/hypercounter", Home())
        with make_server("", 6502, app) as httpd:
            print("Serving on port 6502")
            httpd.serve_forever()
    if args.printer:
        print(f"Yesterdays New Users: {get_yesterdays_asos_users()}")
        print(f"Yesterdays New Transactions: {get_yesterdays_asos_transactions()}")
        print(f"Total Users: {get_all_asos_users_since_27th()}")
        print(f"Total Transactions: {get_all_asos_transactions_since_27th()}")
    if args.teams:
        if is_leader():
            teams_notification()
