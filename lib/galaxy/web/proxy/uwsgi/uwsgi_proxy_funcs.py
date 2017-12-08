import sqlite3
import threading

import uwsgi

db_conn = threading.local()


def dynamic_proxy_mapper(hostname, galaxy_session):
    """Attempt to lookup downstream host from database"""
    if not hasattr(db_conn, 'c'):
        db_conn.c = sqlite3.connect(uwsgi.opt["galaxy-proxy-sessions"])
    if galaxy_session:
        # Order by rowid gives us the last row added
        row = db_conn.c.execute("SELECT key, host, port FROM gxproxy2 WHERE key=?", (galaxy_session,)).fetchone()
        if row:
            return ('%s:%s' % (row[1], row[2])).encode()
    # No match for session found
    return None


uwsgi.register_rpc('dynamic_proxy_mapper', dynamic_proxy_mapper)
