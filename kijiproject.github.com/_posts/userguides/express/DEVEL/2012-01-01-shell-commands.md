---
layout: post
title: KijiExpress Shell Commands
categories: [userguides, express, devel]
tags : [express-ug]
version: devel
order : 8
description: KijiExpress Shell Commands.
---
##DRAFT##

`express> :help`

All commands can be abbreviated: `:he` instead of `:help`.

Those marked with a * have more detailed help: `:help imports`.

?TBW?

`:cp \<path\>`
:Add a jar or directory to the classpath

`:help [command]`
:print this summary or command-specific help

`:history [num]`
:show the history (optional num is commands to show)

`:h? \<string\>`
:search the history

`:imports [name name ...]`
:show import history, identifying sources of names

`:implicits [-v]`
:show the implicits in scope

`:javap \<path|class\>`
:disassemble a file or class name

`:keybindings`
:show how ctrl-\[A-Z\] and other keys are bound

`:load \<path\>`
:load and interpret a Scala file

`:paste`
:enter paste mode: all input up to ctrl-D compiled together

`:power`
:enable power user mode

`:quit`
:exit the interpreter

`:replay`
:reset execution and replay all previous commands

`:sh \<command line\>`
:run a shell command (result is implicitly => List\[String\])

`:silent`
:disable/enable automatic printing of results

`:type \<expr\>`
:display the type of an expression without evaluating it

`:schema-shell`
:Runs the KijiSchema shell.
