# -*- coding: utf-8 -*-

"""
Copyright 2023 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import asyncio
import inspect
from functools import wraps
from typing import Optional, Sequence, TypeVar, Union

import grpc
from dapr.ext.workflow.logger import Logger, LoggerOptions
from dapr.ext.workflow.workflow_activity_context import Activity
from dapr.ext.workflow.workflow_runtime import WorkflowRuntime as WorkflowRuntimeSync

T = TypeVar('T')
TInput = TypeVar('TInput')
TOutput = TypeVar('TOutput')

ClientInterceptor = Union[
    grpc.aio.UnaryUnaryClientInterceptor,
    grpc.aio.UnaryStreamClientInterceptor,
    grpc.aio.StreamUnaryClientInterceptor,
    grpc.aio.StreamStreamClientInterceptor,
]


class WorkflowRuntime(WorkflowRuntimeSync):
    """WorkflowRuntime is the entry point for registering workflows and async activities."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[str] = None,
        logger_options: Optional[LoggerOptions] = None,
        interceptors: Optional[Sequence[ClientInterceptor]] = None,
        maximum_concurrent_activity_work_items: Optional[int] = None,
        maximum_concurrent_orchestration_work_items: Optional[int] = None,
        maximum_thread_pool_workers: Optional[int] = None,
        main_event_loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        super().__init__(
            host,
            port,
            logger_options,
            interceptors,
            maximum_concurrent_activity_work_items,
            maximum_concurrent_orchestration_work_items,
            maximum_thread_pool_workers,
        )
        self._main_event_loop = main_event_loop
        self._logger = Logger('WorkflowRuntime', logger_options)

    def activity(self, __fn: Activity = None, *, name: Optional[str] = None):
        """Decorator to register an async activity function.

        This example shows how to register an activity function with an alternate name:

            from dapr.ext.workflow.aio import WorkflowRuntime
            wfr = WorkflowRuntime()

            @wfr.activity(name="add")
            async def add(ctx, x: int, y: int) -> int:
                return x + y

        This example shows how to register an activity function without an alternate name:

                from dapr.ext.workflow.aio import WorkflowRuntime
                wfr = WorkflowRuntime()

                @wfr.activity
                async def add(ctx, x: int, y: int) -> int:
                    return x + y

        Args:
            name (Optional[str], optional): Name to identify the activity function as in
            the workflow runtime. Defaults to None.
        """

        def wrapper(fn: Activity):
            # If a main event loop is provided, wrap the function so that any awaitable
            # result is executed on that loop in a thread-safe way.
            if self._main_event_loop:

                @wraps(fn)
                def sync_wrapper(*args, **kwargs):
                    result = fn(*args, **kwargs)
                    if inspect.isawaitable(result):
                        future = asyncio.run_coroutine_threadsafe(result, self._main_event_loop)
                        return future.result()
                    return result

                target_fn = sync_wrapper
            else:
                # No special handling needed; register the original function directly.
                @wraps(fn)
                def innerfn():
                    return fn

                target_fn = innerfn

            self.register_activity(target_fn, name=name)

            if hasattr(fn, '_dapr_alternate_name'):
                target_fn.__dict__['_dapr_alternate_name'] = fn.__dict__['_dapr_alternate_name']
            else:
                target_fn.__dict__['_dapr_alternate_name'] = name if name else fn.__name__
            target_fn.__signature__ = inspect.signature(fn)
            # Copy attributes to fn so it doesn't get registered again when calling `register_activity()` again.
            fn.__dict__['_activity_registered'] = target_fn.__dict__['_activity_registered']
            fn.__dict__['_dapr_alternate_name'] = target_fn.__dict__['_dapr_alternate_name']
            return target_fn

        if __fn:
            # This case is true when the decorator is used without arguments
            # and the function to be decorated is passed as the first argument.
            return wrapper(__fn)

        return wrapper
