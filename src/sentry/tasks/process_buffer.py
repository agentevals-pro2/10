import logging

import sentry_sdk
from django.apps import apps
from django.conf import settings

from sentry.tasks.base import instrumented_task
from sentry.utils.locking import UnableToAcquireLock
from sentry.utils.locking.lock import Lock

logger = logging.getLogger(__name__)


def get_process_lock(lock_name: str) -> Lock:
    from sentry.locks import locks

    return locks.get(f"buffer:{lock_name}", duration=60, name=lock_name)


@instrumented_task(
    name="sentry.tasks.process_buffer.process_pending", queue="buffers.process_pending"
)
def process_pending() -> None:
    """
    Process pending buffers.
    """
    from sentry import buffer

    lock = get_process_lock("process_pending")

    try:
        with lock.acquire():
            buffer.process_pending()
    except UnableToAcquireLock as error:
        logger.warning("process_pending.fail", extra={"error": error})


@instrumented_task(
    name="sentry.tasks.process_buffer.process_pending_batch", queue="buffers.process_pending_batch"
)
def process_pending_batch() -> None:
    """
    Process pending buffers in a batch.
    """
    from sentry import buffer

    lock = get_process_lock("process_pending_batch")

    try:
        with lock.acquire():
            buffer.process_batch()
    except UnableToAcquireLock as error:
        logger.warning("process_pending_batch.fail", extra={"error": error})


@instrumented_task(name="sentry.tasks.process_buffer.process_incr", queue="counters-0")
def process_incr(**kwargs):
    """
    Processes a buffer event.
    """
    from sentry import buffer

    sentry_sdk.set_tag("model", kwargs.get("model", "Unknown"))

    buffer.process(**kwargs)


def buffer_incr(model, *args, **kwargs):
    """Schedule :func:`buffer.incr` for the given ``model``.

    ``args`` and ``kwargs`` mirror the parameters of :meth:`Buffer.incr`. The most
    common keyword arguments are ``columns`` (a mapping of column names to
    increments), ``filters`` (used to identify the row to update), ``extra``
    (additional columns to set directly), and ``signal_only`` (skip database
    writes and only emit the ``buffer_incr_complete`` signal).

    If ``SENTRY_BUFFER_INCR_AS_CELERY_TASK`` is enabled the call is queued via
    Celery, otherwise it runs inline.
    """
    (buffer_incr_task.delay if settings.SENTRY_BUFFER_INCR_AS_CELERY_TASK else buffer_incr_task)(
        app_label=model._meta.app_label, model_name=model._meta.model_name, args=args, kwargs=kwargs
    )


@instrumented_task(
    name="sentry.tasks.process_buffer.buffer_incr_task",
    queue="buffers.incr",
)
def buffer_incr_task(app_label, model_name, args, kwargs):
    """Execute :meth:`Buffer.incr` for ``app_label.model_name``.

    ``model_name`` should be the lowercase model identifier as provided by
    Django's ``Model._meta.model_name`` attribute. ``args`` and ``kwargs`` are
    forwarded directly to :meth:`Buffer.incr`.
    """
    from sentry import buffer

    sentry_sdk.set_tag("model", model_name)

    buffer.incr(apps.get_model(app_label=app_label, model_name=model_name), *args, **kwargs)
