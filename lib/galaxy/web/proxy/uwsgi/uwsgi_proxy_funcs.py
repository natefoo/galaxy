import uwsgi
import sqlite3

db_conn = sqlite3.connect(uwsgi.opt["sessions"])


def dynamic_proxy_mapper(hostname, galaxy_session):
    """Attempt to lookup downstream host from database"""
    if galaxy_session:
        # Order by rowid gives us the last row added
        row = db_conn.execute("SELECT key, host, port FROM gxproxy2 WHERE key=?", (galaxy_session,)).fetchone()
        if row:
            return ('%s:%s' % (row[1], row[2])).encode()
    # No match for session found
    return None


uwsgi.register_rpc('dynamic_proxy_mapper', dynamic_proxy_mapper)
