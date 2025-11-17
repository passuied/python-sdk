# -*- coding: utf-8 -*-

"""
Copyright 2025 The Dapr Authors
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

    def register_activity(self, fn: Activity, *, name: Optional[str] = None):
        """Registers an async workflow activity and ensures proper execution and metadata.
        This mirrors the decorator behavior so direct registration behaves identically.
        """
        # Validate/prepare alternate name on the original function first (not the wrapper)
        if hasattr(fn, '_activity_registered'):
            alt_name = fn.__dict__['_dapr_alternate_name']
            raise ValueError(f'Activity {fn.__name__} already registered as {alt_name}')
        if hasattr(fn, '_dapr_alternate_name'):
            alt_name = fn._dapr_alternate_name
            if name is not None:
                raise ValueError(f'Activity {fn.__name__} already has an alternate name {alt_name}')
        else:
            alt_name = name if name else fn.__name__
            fn.__dict__['_dapr_alternate_name'] = alt_name

        # Build the target function that super().register_activity will wrap.
        if self._main_event_loop:

            @wraps(fn)
            def target_fn(*args, **kwargs):
                result = fn(*args, **kwargs)
                if inspect.isawaitable(result):
                    future = asyncio.run_coroutine_threadsafe(result, self._main_event_loop)
                    return future.result()
                return result
        else:

            @wraps(fn)
            def target_fn():
                return fn

        # Delegate to base registration without passing name because the wrapper already
        # carries _dapr_alternate_name via @wraps(fn) copying fn.__dict__.
        super().register_activity(target_fn, name=None)
        fn.__dict__['_activity_registered'] = True
        fn.__dict__['_dapr_alternate_name'] = alt_name

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
            # Register first to ensure original fn has flags set
            self.register_activity(fn, name=name)

            @wraps(fn)
            def innerfn():
                return fn

            # Mirror naming metadata on the returned decorator wrapper
            if hasattr(fn, '_dapr_alternate_name'):
                innerfn.__dict__['_dapr_alternate_name'] = fn.__dict__['_dapr_alternate_name']
            else:
                innerfn.__dict__['_dapr_alternate_name'] = name if name else fn.__name__
            innerfn.__signature__ = inspect.signature(fn)
            return innerfn

        if __fn:
            # This case is true when the decorator is used without arguments
            # and the function to be decorated is passed as the first argument.
            return wrapper(__fn)

        return wrapper
