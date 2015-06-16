#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Unit-tests for task I/O."""

import logging
import os
import time

from base import base
from base import unittest

from workflow import workflow


# --------------------------------------------------------------------------------------------------


class Task1(workflow.IOTask):
    def run_with_io(self, output):
        logging.info('Task1 is running NOW')
        output.timestamp = base.timestamp()
        return self.SUCCESS


class Task2(workflow.IOTask):
    def get_task_run_id(self):
        return '.'.join([self.task_id, 'task1:timestamp=%s' % self.input.task1.timestamp])

    def run_with_io(self, output, task1):
        logging.info('Task2 input task1=%r' % task1)
        return self.SUCCESS


# --------------------------------------------------------------------------------------------------


class TestWorkflow(unittest.BaseTestCase):
    def test_task_io(self):
        try:
            flow = workflow.Workflow()
            task1 = Task1(workflow=flow, task_id='task1')
            task2 = Task2(workflow=flow, task_id='task2')

            task2.bind_input_to_task_output('task1', task1)

            flow.build()
            flow.process(nworkers=10)

            self.assertEqual(workflow.TaskState.SUCCESS, task1.state)
            self.assertEqual(workflow.TaskState.SUCCESS, task2.state)

            flow2 = workflow.Workflow()
            task1_again = Task1(workflow=flow2, task_id='task1')
            task2_again = Task2(workflow=flow2, task_id='task2')
            task2_again.bind_input_to_task_output('task1', task1_again)
            task3 = Task1(workflow=flow2, task_id='task3', runs_after=[task2_again])
            flow2.build()
            flow2.process(nworkers=10)
            for original, again in ((task1, task1_again), (task2, task2_again)):
                self.assertEqual(workflow.TaskState.ALREADY_DONE, again.state)
                self.assertEqual(original.start_time, again.start_time)
                self.assertEqual(original.end_time, again.end_time)
            self.assertEqual(workflow.TaskState.SUCCESS, task3.state)
        finally:
            os.remove(task1.get_task_run_id())
            os.remove(task2.get_task_run_id())
            os.remove(task3.get_task_run_id())

    def test_should_task_run(self):
        class TestTask(workflow.IOTask):
            def __init__(self, should_task_run=True, **kwargs):
                super().__init__(**kwargs)
                self._should_counter = 0
                self._run_counter = 0
                self._should_task_run = should_task_run

            def should_task_run(self, task_run_id, output, **inputs):
                self._should_counter += 1
                return self._should_task_run

            def run_with_io(self, output, **inputs):
                self._run_counter += 1
                output.time = time.time()
                return self.SUCCESS

        flow = workflow.Workflow()
        task = TestTask(workflow=flow, task_id="TestTask")
        flow.build()
        flow.process()

        self.assertEqual(0, task._should_counter)
        self.assertEqual(1, task._run_counter)

        flow = workflow.Workflow()
        task = TestTask(workflow=flow, task_id="TestTask", should_task_run=True)
        flow.build()
        flow.process()

        self.assertEqual(1, task._should_counter)
        self.assertEqual(1, task._run_counter)

        flow = workflow.Workflow()
        task = TestTask(workflow=flow, task_id="TestTask", should_task_run=False)
        flow.build()
        flow.process()

        self.assertEqual(1, task._should_counter)
        self.assertEqual(0, task._run_counter)

        os.remove(task.get_task_run_id())


# --------------------------------------------------------------------------------------------------


if __name__ == '__main__':
    base.run(unittest.base_main)
