#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Unit-tests for Workflow and Task."""

import http.client
import logging
import time
from unittest import mock

from base import base
from base import unittest
from workflow import workflow


FLAGS = base.FLAGS

FLAGS.add_float(
    name='slow_task_duration',
    default=1.0,
    help='Duration of slow tasks, in seconds.',
)


class Error(Exception):
    """Errors used in this module."""
    pass


class SlowTask(workflow.Task):
    def run(self):
        time.sleep(FLAGS.slow_task_duration)
        return self.SUCCESS


class SuccessTask(workflow.Task):
    def run(self):
        return self.SUCCESS


class FailureTask(workflow.Task):
    def run(self):
        return self.FAILURE


class ErrorTask(workflow.Task):
    def run(self):
        raise workflow.Error("ErrorTask")


class TestWorkflow(unittest.BaseTestCase):
    def test_empty_workflow(self):
        flow = workflow.Workflow()
        flow.build()
        flow.process()

    def testLinearWorkflowSuccess(self):
        flow = workflow.Workflow()
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = SuccessTask(workflow=flow, task_id='task2', runs_after=[task1])
        task3 = SuccessTask(workflow=flow, task_id='task3', runs_after=[task2])
        task4 = SuccessTask(workflow=flow, task_id='task4', runs_after=[task3])
        task5 = SuccessTask(workflow=flow, task_id='task5', runs_after=[task4])
        flow.build()
        flow.process(nworkers=10)
        self.assertEqual(workflow.TaskState.SUCCESS, task1.state)
        self.assertEqual(workflow.TaskState.SUCCESS, task2.state)
        self.assertEqual(workflow.TaskState.SUCCESS, task3.state)
        self.assertEqual(workflow.TaskState.SUCCESS, task4.state)
        self.assertEqual(workflow.TaskState.SUCCESS, task5.state)

        print(flow.dump_as_dot())

    def testParallelWorkflowSuccess(self):
        flow = workflow.Workflow()
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = SuccessTask(workflow=flow, task_id='task2')
        task3 = SuccessTask(workflow=flow, task_id='task3')
        task4 = SuccessTask(workflow=flow, task_id='task4')
        task5 = SuccessTask(workflow=flow, task_id='task5')
        flow.build()
        flow.process(nworkers=10)
        self.assertEqual(workflow.TaskState.SUCCESS, task1.state)
        self.assertEqual(workflow.TaskState.SUCCESS, task2.state)
        self.assertEqual(workflow.TaskState.SUCCESS, task3.state)
        self.assertEqual(workflow.TaskState.SUCCESS, task4.state)
        self.assertEqual(workflow.TaskState.SUCCESS, task5.state)

    def testLinearWorkflowFailure(self):
        flow = workflow.Workflow()
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = FailureTask(workflow=flow, task_id='task2', runs_after=[task1])
        task3 = SuccessTask(workflow=flow, task_id='task3', runs_after=[task2])
        task4 = SuccessTask(workflow=flow, task_id='task4', runs_after=[task3])
        task5 = SuccessTask(workflow=flow, task_id='task5', runs_after=[task4])
        flow.build()
        flow.process(nworkers=10)
        self.assertEqual(workflow.TaskState.SUCCESS, task1.state)
        self.assertEqual(workflow.TaskState.FAILURE, task2.state)
        self.assertEqual(workflow.TaskState.FAILURE, task3.state)
        self.assertEqual(workflow.TaskState.FAILURE, task4.state)
        self.assertEqual(workflow.TaskState.FAILURE, task5.state)

    def testWorkflowFailure(self):
        flow = workflow.Workflow()
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = FailureTask(workflow=flow, task_id='task2', runs_after=[task1])
        task3 = SuccessTask(workflow=flow, task_id='task3', runs_after=[task2])
        task4 = SuccessTask(workflow=flow, task_id='task4', runs_after=[task2, task3])
        flow.build()
        flow.process(nworkers=10)
        self.assertEqual(workflow.TaskState.SUCCESS, task1.state)
        self.assertEqual(workflow.TaskState.FAILURE, task2.state)
        self.assertEqual(workflow.TaskState.FAILURE, task3.state)
        self.assertEqual(workflow.TaskState.FAILURE, task4.state)

    def testCircularDep(self):
        flow = workflow.Workflow()
        task1 = SuccessTask(workflow=flow, task_id='task1', runs_after=['task3'])
        task2 = FailureTask(workflow=flow, task_id='task2', runs_after=[task1])
        task3 = SuccessTask(workflow=flow, task_id='task3', runs_after=[task2])
        try:
            flow.build()
            self.fail("circular dependency task1->%s, task2->%s, task3->%s"
                      % (task1.runs_after, task2.runs_after, task3.runs_after))
        except workflow.CircularDependencyError:
            pass

    def testCircularDep2(self):
        flow = workflow.Workflow()
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = FailureTask(workflow=flow, task_id='task2')
        task3 = SuccessTask(workflow=flow, task_id='task3')
        task1.must_run_before(task2)
        task2.must_run_after(task3)
        task2.must_run_before(task3)
        try:
            flow.build()
            self.fail()
        except workflow.CircularDependencyError:
            pass

    def test_dump_as_svg(self):
        flow1 = workflow.Workflow()
        task1 = SuccessTask(workflow=flow1, task_id='task1')
        task2 = FailureTask(workflow=flow1, task_id='task2')
        task3 = SuccessTask(workflow=flow1, task_id='task3')
        task2.must_run_after(task1)
        task3.must_run_after(task2)
        flow1.build()

        svg_source = flow1.dump_as_svg()
        logging.debug('SVG source:\n%s', svg_source)

        server = workflow.WorkflowHTTPMonitor(interface='127.0.0.1', port=0)
        server.start()
        try:
            flow1.process()

            conn = http.client.HTTPConnection(host='127.0.0.1', port=server.server_port)
            conn.connect()
            try:
                conn.request(method='GET', url='')
                response = conn.getresponse()
                self.assertEqual(404, response.getcode())

                server.set_workflow(flow1)

                conn.request(method='GET', url='/svg')
                response = conn.getresponse()
                self.assertEqual(200, response.getcode())
                self.assertEqual('image/svg+xml', response.getheader('Content-Type'))
                logging.debug('HTTP response: %r', response.read())
            finally:
                conn.close()

        finally:
            server.stop()

    def test_dump_state_as_table(self):
        flow1 = workflow.Workflow()
        task1 = SlowTask(workflow=flow1, task_id='task1')
        task2 = SlowTask(workflow=flow1, task_id='task2')
        task3 = SlowTask(workflow=flow1, task_id='task3')
        task2.must_run_after(task1)
        task3.must_run_after(task2)
        flow1.build()

        svg_source = flow1.dump_state_as_table()
        logging.debug('Workflow table:\n%s', svg_source)

        server = workflow.WorkflowHTTPMonitor(interface='127.0.0.1', port=0)
        server.start()
        server.set_workflow(flow1)
        try:
            flow1.process()

            conn = http.client.HTTPConnection(
                host='127.0.0.1',
                port=server.server_port,
            )
            conn.connect()
            try:
                conn.request(method='GET', url='')
                response = conn.getresponse()
                self.assertEqual(200, response.getcode())
                self.assertEqual('text/plain', response.getheader('Content-Type'))
                logging.debug('HTTP response: %r', response.read())
            finally:
                conn.close()

        finally:
            server.stop()

    def disabled_test_workflow_diff(self):
        flow1 = workflow.Workflow()
        task1 = SuccessTask(workflow=flow1, task_id='task1')
        task2 = SuccessTask(workflow=flow1, task_id='task2')
        task3 = SuccessTask(workflow=flow1, task_id='task3')
        task2.must_run_after(task1)
        task3.must_run_after(task2)
        flow1.build()

        flow2 = workflow.Workflow()
        task1 = SuccessTask(workflow=flow2, task_id='task1')
        task2b = SuccessTask(workflow=flow2, task_id='task2b')
        task3 = SuccessTask(workflow=flow2, task_id='task3')
        task2b.must_run_after(task1)
        task3.must_run_after(task2b)
        task3.must_run_after(task1)
        flow2.build()

        workflow.diff_workflow(flow1, flow2)

    def test_get_upstream_tasks(self):
        flow = workflow.Workflow()
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = SuccessTask(workflow=flow, task_id='task2')
        task3 = SuccessTask(workflow=flow, task_id='task3')
        task4 = SuccessTask(workflow=flow, task_id='task4')
        task5 = SuccessTask(workflow=flow, task_id='task5')
        task6 = SuccessTask(workflow=flow, task_id='task6')
        task7 = SuccessTask(workflow=flow, task_id='task7')

        task3.must_run_after(task1)
        task3.must_run_after(task2)
        task4.must_run_after(task3)
        task5.must_run_after(task3)

        task7.must_run_after(task6)

        self.assertEqual(
            set({task1}),
            workflow.get_upstream_tasks(flow, [task1]))

        self.assertEqual(
            set({task1, task2, task3}),
            workflow.get_upstream_tasks(flow, [task3]))

        self.assertEqual(
            set({task1, task2, task3, task4}),
            workflow.get_upstream_tasks(flow, [task4]))

        self.assertEqual(
            set({task1, task2, task3, task5}),
            workflow.get_upstream_tasks(flow, [task5]))

        self.assertEqual(
            set({task1, task2, task3, task4, task5}),
            workflow.get_upstream_tasks(flow, [task4, task5]))

        self.assertEqual(
            set({task1, task2, task3, task4, task5}),
            workflow.get_upstream_tasks(flow, [task3, task4, task5]))

        self.assertEqual(
            set({task6}),
            workflow.get_upstream_tasks(flow, [task6]))

        self.assertEqual(
            set({task6, task7}),
            workflow.get_upstream_tasks(flow, [task7]))

        self.assertEqual(
            set({task1, task2, task3, task4, task6, task7}),
            workflow.get_upstream_tasks(flow, [task4, task7]))

    def test_get_downstream_tasks(self):
        flow = workflow.Workflow()
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = SuccessTask(workflow=flow, task_id='task2')
        task3 = SuccessTask(workflow=flow, task_id='task3')
        task4 = SuccessTask(workflow=flow, task_id='task4')
        task5 = SuccessTask(workflow=flow, task_id='task5')
        task6 = SuccessTask(workflow=flow, task_id='task6')
        task7 = SuccessTask(workflow=flow, task_id='task7')

        task3.must_run_after(task1)
        task3.must_run_after(task2)
        task4.must_run_after(task3)
        task5.must_run_after(task3)

        task7.must_run_after(task6)

        self.assertEqual(
            set({task1, task3, task4, task5}),
            workflow.get_downstream_tasks(flow, [task1]))

        self.assertEqual(
            set({task3, task4, task5}),
            workflow.get_downstream_tasks(flow, [task3]))

        self.assertEqual(
            set({task4}),
            workflow.get_downstream_tasks(flow, [task4]))

        self.assertEqual(
            set({task5}),
            workflow.get_downstream_tasks(flow, [task5]))

        self.assertEqual(
            set({task4, task5}),
            workflow.get_downstream_tasks(flow, [task4, task5]))

        self.assertEqual(
            set({task3, task4, task5}),
            workflow.get_downstream_tasks(flow, [task3, task4, task5]))

        self.assertEqual(
            set({task6, task7}),
            workflow.get_downstream_tasks(flow, [task6]))

        self.assertEqual(
            set({task7}),
            workflow.get_downstream_tasks(flow, [task7]))

        self.assertEqual(
            set({task3, task4, task5, task6, task7}),
            workflow.get_downstream_tasks(flow, [task3, task6]))

    def testAddDep(self):
        """Tests that forward declaration of dependencies are properly set."""
        flow = workflow.Workflow()
        flow.AddDep('task1', 'task2')
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = SuccessTask(workflow=flow, task_id='task2')
        self.assertTrue('task1' in task2.runs_after)
        self.assertTrue('task2' in task1.runs_before)

    def test_prune(self):
        """Tests that forward declaration of dependencies are properly set."""
        flow = workflow.Workflow()
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = SuccessTask(workflow=flow, task_id='task2')
        task3 = SuccessTask(workflow=flow, task_id='task3')
        task4 = SuccessTask(workflow=flow, task_id='task4')
        task5 = SuccessTask(workflow=flow, task_id='task5')
        task6 = SuccessTask(workflow=flow, task_id='task6')
        task7 = SuccessTask(workflow=flow, task_id='task7')

        task3.must_run_after(task1)
        task3.must_run_after(task2)
        task4.must_run_after(task3)
        task5.must_run_after(task3)

        task7.must_run_after(task6)

        flow.prune(tasks={task4}, direction=workflow.UPSTREAM)
        self.assertEqual({'task1', 'task2', 'task3', 'task4'}, flow.tasks.keys())

    def test_workflow_task_error_calls_shutdown(self):
        real_shutdown = base.shutdown
        try:
            shutdown_count = 0
            flow = workflow.Workflow()

            def mock_shutdown():
                nonlocal shutdown_count
                nonlocal flow
                logging.info("base.shutdown called")
                shutdown_count += 1
                flow._done.set()
            base.shutdown = mock_shutdown

            task1 = SuccessTask(workflow=flow, task_id='task1')
            task2 = ErrorTask(workflow=flow, task_id="task2", runs_after=[task1])
            task3 = SuccessTask(workflow=flow, task_id="task3", runs_after=[task2])
            flow.build()
            flow.process(nworkers=10)

            self.assertTrue(shutdown_count > 0, "shutdown should have been called on error")
            self.assertEqual(workflow.TaskState.SUCCESS, task1.state)
            # task2.state is undefined because base.shutdown didn't shutdown
            self.assertEqual(workflow.TaskState.PENDING, task3.state)
        finally:
            base.shutdown = real_shutdown

    def test_workflow_task_error_handler(self):
        task_error_kwargs = None

        def handle_task_error(**kwargs):
            nonlocal task_error_kwargs
            task_error_kwargs = kwargs
            return workflow.TaskState.SUCCESS

        flow = workflow.Workflow(task_error_handler=handle_task_error)
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = ErrorTask(workflow=flow, task_id="task2", runs_after=[task1])
        task3 = SuccessTask(workflow=flow, task_id="task3", runs_after=[task2])
        flow.build()
        flow.process(nworkers=10)

        self.assertEqual(flow, task_error_kwargs["flow"])
        self.assertEqual(task2, task_error_kwargs["task"])
        self.assertEqual(workflow.Error, type(task_error_kwargs["error"]))

        self.assertEqual(workflow.TaskState.SUCCESS, task1.state)
        self.assertEqual(workflow.TaskState.SUCCESS, task2.state)
        self.assertEqual(workflow.TaskState.SUCCESS, task3.state)

    def test_completion_handler_on_success(self):
        """Tests that the completion handler is run after a successful workflow run."""
        completion_handler_workflow_pointer = None

        def test_completion_handler(workflow):
            """Function to run after the workflow completes.

            Args:
                workflow: The workflow that completed."""
            nonlocal completion_handler_workflow_pointer
            completion_handler_workflow_pointer = workflow

        flow = workflow.Workflow()
        flow.add_completion_handler(test_completion_handler)
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = SuccessTask(workflow=flow, task_id='task2', runs_after=[task1])
        flow.build()
        flow.process(nworkers=3)
        self.assertEqual(workflow.TaskState.SUCCESS, task1.state)
        self.assertEqual(workflow.TaskState.SUCCESS, task2.state)

        self.assertIs(flow, completion_handler_workflow_pointer)

    def test_completion_handler_empty_workflow(self):
        """Tests that the completion handler is run after an empty workflow run."""
        completion_handler_workflow_pointer = None

        def test_completion_handler(workflow):
            """Function to run after the workflow completes.

            Args:
                workflow: The workflow that completed."""
            nonlocal completion_handler_workflow_pointer
            completion_handler_workflow_pointer = workflow

        flow = workflow.Workflow()
        flow.add_completion_handler(test_completion_handler)
        flow.build()
        flow.process(nworkers=3)

        self.assertIs(flow, completion_handler_workflow_pointer)

    def test_completion_handler_on_failure(self):
        """Tests that the completion handler is run after an unsuccessful workflow run."""
        completion_handler_workflow_pointer = None

        def test_completion_handler(workflow):
            """Function to run after the workflow completes.

            Args:
                workflow: The workflow that completed."""
            nonlocal completion_handler_workflow_pointer
            completion_handler_workflow_pointer = workflow

        flow = workflow.Workflow()
        flow.add_completion_handler(test_completion_handler)
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = FailureTask(workflow=flow, task_id='task2', runs_after=[task1])
        flow.build()
        flow.process(nworkers=10)
        self.assertEqual(workflow.TaskState.SUCCESS, task1.state)
        self.assertEqual(workflow.TaskState.FAILURE, task2.state)

        self.assertIs(flow, completion_handler_workflow_pointer)

    def test_completion_handlers_multiple(self):
        """Tests that multiple completion handlers run after a successful workflow run."""
        completion_handler_1 = mock.Mock()
        completion_handler_2 = mock.Mock()

        flow = workflow.Workflow()
        flow.add_completion_handler(completion_handler_1)
        flow.add_completion_handler(completion_handler_2)
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = SuccessTask(workflow=flow, task_id='task2', runs_after=[task1])
        flow.build()
        flow.process(nworkers=3)
        self.assertEqual(workflow.TaskState.SUCCESS, task1.state)
        self.assertEqual(workflow.TaskState.SUCCESS, task2.state)

        self.assertEqual(1, completion_handler_1.call_count)
        self.assertEqual(1, completion_handler_2.call_count)

    def test_completion_handler_exceptions(self):
        """Tests a failed completion handler does not prevent others from running."""
        completion_handler_workflow_pointer = None

        def failing_completion_handler(workflow):
            """Function to run after the workflow completes.

            Args:
                workflow: The workflow that completed."""
            nonlocal completion_handler_workflow_pointer
            completion_handler_workflow_pointer = workflow
            raise Error("This completion handler is supposed to error.")

        completion_handler = mock.Mock()

        flow = workflow.Workflow()
        flow.add_completion_handler(failing_completion_handler)
        flow.add_completion_handler(completion_handler)
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = FailureTask(workflow=flow, task_id='task2', runs_after=[task1])
        flow.build()
        flow.process(nworkers=10)
        self.assertEqual(workflow.TaskState.SUCCESS, task1.state)
        self.assertEqual(workflow.TaskState.FAILURE, task2.state)

        self.assertIs(flow, completion_handler_workflow_pointer)
        self.assertEqual(1, completion_handler.call_count)

    def test_workflow_async(self):
        """Tests that a workflow can run successfully asynchronously, and that the completion
        handler also runs."""
        completion_handler_workflow_pointer = None

        def test_completion_handler(workflow):
            """Function to run after the workflow completes.

            Args:
                workflow: The workflow that completed."""
            nonlocal completion_handler_workflow_pointer
            completion_handler_workflow_pointer = workflow

        flow = workflow.Workflow()
        flow.add_completion_handler(completion_handler=test_completion_handler)
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = SuccessTask(workflow=flow, task_id='task2', runs_after=[task1])
        flow.build()
        flow.process(nworkers=3, sync=False)
        flow.wait()
        self.assertEqual(workflow.TaskState.SUCCESS, task1.state)
        self.assertEqual(workflow.TaskState.SUCCESS, task2.state)

        self.assertIs(flow, completion_handler_workflow_pointer)


# ------------------------------------------------------------------------------

if __name__ == '__main__':
    base.run(unittest.base_main)
