from operator import itemgetter

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from .queues import get_unique_connection_configs

from redis import Redis
from rq import Worker, use_connection


SHOW_ADMIN_LINK = getattr(settings, 'RQ_SHOW_ADMIN_LINK', False)

REDIS_SERVERS = getattr(settings, 'REDIS_SERVERS', None)
RQ_QUEUES = {}
for redis_server in REDIS_SERVERS:
    redis = Redis(
        host=redis_server.get('HOST'),
        port=redis_server.get('PORT'),
        db=redis_server.get('DB'),
        password=redis_server.get('PASSWORD'),
    )
    use_connection(redis)
    for worker in Worker.all():
        for queue in worker.queues:
            RQ_QUEUES[queue.name] = {
                'HOST': redis_server.get('HOST'),
                'PORT': redis_server.get('PORT'),
                'DB': redis_server.get('DB'),
                'PASSWORD': redis_server.get('PASSWORD'),
            }


QUEUES = getattr(settings, 'RQ_QUEUES', RQ_QUEUES)
if QUEUES is None:
    raise ImproperlyConfigured("You have to define RQ_QUEUES in settings.py")
NAME = getattr(settings, 'RQ_NAME', 'default')
BURST = getattr(settings, 'RQ_BURST', False)

# All queues in list format so we can get them by index, includes failed queues
QUEUES_LIST = []
for key, value in sorted(QUEUES.items(), key=itemgetter(0)):
    QUEUES_LIST.append({'name': key, 'connection_config': value})
for config in get_unique_connection_configs():
    QUEUES_LIST.append({'name': 'failed', 'connection_config': config})

# Get exception handlers
EXCEPTION_HANDLERS = getattr(settings, 'RQ_EXCEPTION_HANDLERS', [])
