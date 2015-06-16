#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Test for subworkflows and related functionality"""

from base import base
from base import unittest
from workflow import workflow


# --------------------------------------------------------------------------------------------------


ALREADY_DONE = workflow.TaskState.ALREADY_DONE
SUCCESS = workflow.TaskState.SUCCESS


class DummyTask(workflow.Task):
    def __init__(self, append_to, run_return=SUCCESS, **kwargs):
        super().__init__(**kwargs)
        self._append_to = append_to
        self._run_return = run_return

    def run(self):
        self._append_to.append(self.task_id)
        return self._run_return


# --------------------------------------------------------------------------------------------------


class TestSubWorkflow(unittest.BaseTestCase):

    def setUp(self):
        self.flow = workflow.Workflow(name="task_test")
        self.run_order = []

    def make_dummy_task(self, workflow=None, *args, **kwargs):
        if workflow is None:
            workflow = self.flow
        return DummyTask(*args, append_to=self.run_order, workflow=workflow, **kwargs)

    def process_flow(self, nworkers=10, monitor_thread=False, sync=True):
        self.flow.build()
        return self.flow.process(nworkers=nworkers, monitor_thread=monitor_thread, sync=sync)

    def test_sequence(self):
        workflow.make_task_sequence(
            ["a", "b", "c"],
            lambda task_id: self.make_dummy_task(task_id=task_id)
        )
        self.assertTrue(self.process_flow())
        self.assertListEqual(["a", "b", "c"], self.run_order)

    def test_barrier(self):
        barrier = workflow.Barrier(workflow=self.flow, task_id="barrier")
        self.assertEqual(workflow.TaskState.INIT, barrier.state)
        self.assertTrue(self.process_flow())
        self.assertEqual(ALREADY_DONE, barrier.state)

    def test_barrier_bookends(self):
        bookends = workflow.Barrier.make_bookends(
            name="x",
            workflow=self.flow,
            sequence=("a", "b", "c"))
        self.assertEqual(3, len(bookends))
        self.assertListEqual(["x-a", "x-b", "x-c"], [b.task_id for b in bookends])
        self.make_dummy_task(task_id="d...", runs_after=bookends[2:3])
        self.make_dummy_task(task_id="ab", runs_after=bookends[0:1], runs_before=bookends[1:2])
        self.make_dummy_task(task_id="bc", runs_after=bookends[1:2], runs_before=bookends[2:3])

        self.assertTrue(self.process_flow())
        self.assertListEqual([True]*3, [b.state == ALREADY_DONE for b in bookends])
        self.assertListEqual(["ab", "bc", "d..."], self.run_order)

    def test_subworkflow(self):
        sub1 = workflow.SubWorkflow(workflow=self.flow, name="sub1")
        sub2 = workflow.SubWorkflow(workflow=self.flow, name="sub2")

        task1 = self.make_dummy_task(workflow=sub1, task_id="in_sub1")
        task2 = self.make_dummy_task(workflow=sub2, task_id="in_sub2")

        self.assertEqual(task1.workflow, task2.workflow)

        sub1.must_run_before(sub2)
        task1.must_run_before(task2)

        self.assertSetEqual({"sub2-begin"}, sub1.runs_before)
        self.assertSetEqual({"sub1-end"}, sub2.runs_after)

        self.assertTrue(self.process_flow())
        self.assertListEqual(["in_sub1", "in_sub2"], self.run_order)

    def test_circular_subworkflow(self):
        sub1 = workflow.SubWorkflow(workflow=self.flow, name="sub1")
        sub2 = workflow.SubWorkflow(workflow=self.flow, name="sub2")

        task1 = self.make_dummy_task(workflow=sub1, task_id="in_sub1")
        task2 = self.make_dummy_task(workflow=sub2, task_id="in_sub2")

        self.assertEqual(task1.workflow, task2.workflow)

        sub1.must_run_before(sub2)
        task2.must_run_before(task1)

        self.assertRaises(workflow.CircularDependencyError, self.flow.build)

    def test_subworkflow_with_begin_end(self):
        tasks = workflow.make_task_sequence(
            ["a", "b", "c", "d"],
            lambda task_id: self.make_dummy_task(task_id=task_id)
        )
        sub1 = workflow.SubWorkflow(workflow=self.flow, name="sub1", begin=tasks[0], end=tasks[1])
        sub2 = workflow.SubWorkflow(workflow=self.flow, name="sub1", begin=tasks[2], end=tasks[3])
        self.make_dummy_task(workflow=sub1, task_id="in_sub1")
        self.make_dummy_task(workflow=sub2, task_id="in_sub2")
        first_task = self.make_dummy_task(task_id="first")
        sub1.must_run_after(first_task)
        last_task = self.make_dummy_task(task_id="last")
        sub2.must_run_before(last_task)

        self.assertSetEqual({"first"}, sub1.runs_after)
        self.assertSetEqual({"c"}, sub1.runs_before)
        self.assertSetEqual({"b"}, sub2.runs_after)
        self.assertSetEqual({"last"}, sub2.runs_before)
        # sub1 and sub2 are already ordered since their begin and ends are from the same sequence
        self.assertTrue(self.process_flow())
        self.assertListEqual(
            ["first", "a", "in_sub1", "b", "c", "in_sub2", "d", "last"],
            self.run_order
        )

    def test_sub_subworkflow(self):
        sub1 = workflow.SubWorkflow(workflow=self.flow, name="sub1")
        sub2 = workflow.SubWorkflow(workflow=self.flow, name="sub2")

        sub_sub1 = workflow.SubWorkflow(workflow=sub1, name="sub_sub1")

        task1 = self.make_dummy_task(workflow=sub1, task_id="in_sub1")
        task2 = self.make_dummy_task(workflow=sub1, task_id="in_sub1_again")
        sub_task = self.make_dummy_task(workflow=sub_sub1, task_id="in_sub_sub1")
        sub_sub1.must_run_after(task1)
        sub_sub1.must_run_before(task2)
        task3 = self.make_dummy_task(workflow=sub2, task_id="in_sub2")

        # All tasks point to the real workflow
        self.assertSetEqual({self.flow}, set(t.workflow for t in (task1, task2, sub_task, task3)))

        sub1.must_run_before(sub2)
        task1.must_run_before(task2)

        self.assertTrue(self.process_flow())
        self.assertListEqual(["in_sub1", "in_sub_sub1", "in_sub1_again", "in_sub2"], self.run_order)

# --------------------------------------------------------------------------------------------------


if __name__ == '__main__':
    base.run(unittest.base_main)
