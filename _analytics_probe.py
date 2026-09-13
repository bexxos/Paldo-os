import os, sqlite3, json, datetime as dt
from pathlib import Path
DB = os.environ.get('PALDO_DB_PATH') or str(Path(__file__).resolve().parent / 'data' / 'paldo_os_outbound.sqlite3')
c = sqlite3.connect(DB)
c.row_factory = sqlite3.Row

def sample(sql, label):
    print('---', label)
    try:
        for r in c.execute(sql).fetchall():
            print(dict(r))
    except Exception as e:
        print('ERR', e)

sample("select id, event_type, entity_type, entity_id, created_at from events order by id desc limit 5", 'events tail')
sample("select id, source, mode, status, started_at, completed_at from discovery_runs order by id desc limit 5", 'discovery_runs tail')
sample("select min(created_at) mn, max(created_at) mx, count(*) n from events", 'events range')
sample("select count(*) n from events", 'events count')
sample("select key, value from system_config order by key", 'system_config')
sample("select id, name, geography, status, campaign_timezone from campaigns", 'campaigns')
sample("select distinct event_type from events order by event_type", 'event types')
