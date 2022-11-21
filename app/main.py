import uvicorn as uvicorn
from celery.result import AsyncResult
from fastapi.openapi.utils import get_openapi
from fastapi.responses import ORJSONResponse
from starlette.responses import JSONResponse

from worker.celery_worker import create_task, my_celery
from fastapi import FastAPI

app = FastAPI(
    docs_url='/openapi',
    openapi_url='/openapi.json',
    default_response_class=ORJSONResponse,
)


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Celery",
        version="1.0.0",
        description="Celery control",
        routes=app.routes,

    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi


@app.get("/{time_sleep}")
async def root(
        time_sleep: int,
):
    task = create_task.delay(time_sleep)
    return JSONResponse({"task_id": task.id})


@app.get("/tasks/{task_id}")
async def get_status(task_id):
    task_result = AsyncResult(task_id)
    result = {
        "task_id": task_id,
        "task_status": task_result.status,
        "task_result": task_result.result,
        "task_date_done": str(task_result.date_done),
    }

    return JSONResponse(result)


@app.get("/celery_info/{}")
async def get_celery_info():
    celery_info = my_celery.control.inspect()
    task_active = dict(celery_info.active())
    task_reserved = dict(celery_info.reserved())

    task_active_count = 0
    task_reserved_count = 0
    for key in task_active.keys():
        task_active_count += len(task_active[key])

    for key in task_reserved.keys():
        task_reserved_count += len(task_reserved[key])

    info = {
        "task_active_count": task_active_count,
        "task_reserved_count": task_reserved_count,
        "task_active": task_active,
        "task_reserved": task_reserved,
    }

    return JSONResponse(info)


if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        host='0.0.0.0',
        port=8000,
    )

# celery worker --app=worker.celery_worker --concurrency= 2 --loglevel=info
# flower --app=worker.celery_worker --port=5556 --broker=redis://localhost:6379/0
