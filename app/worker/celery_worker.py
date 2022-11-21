import time

from celery import Celery

my_celery = Celery(__name__)
default_config = 'worker.celery_config'
my_celery.config_from_object(default_config)


@my_celery.task(name="create_task")
def create_task(task_type):
    time.sleep(int(task_type) * 10)
    return "Потенциально здоровенный текст, который ляжет в Redis"
