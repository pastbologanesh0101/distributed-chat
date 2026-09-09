"""Quiet the noisy 'Task was destroyed but it is pending' / 'Event loop is
closed' chatter that asyncio prints at interpreter shutdown. The integration
tests already cancel their tasks and close their loops; this only hides the
cosmetic GC-time messages so CI logs stay readable."""

import asyncio
import logging


def pytest_configure(config):
    logging.getLogger("asyncio").setLevel(logging.CRITICAL)


def pytest_unconfigure(config):
    # best-effort: drop references so __del__ warnings don't fire on exit
    asyncio.set_event_loop(None)
