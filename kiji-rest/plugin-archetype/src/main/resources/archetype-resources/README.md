#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
Kiji Rest Plugin Skeleton Project version ${project.version}

  (c) Copyright 2013 WibiData, Inc.

  See the NOTICE file distributed with this work for additional
  information regarding copyright ownership.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

This is a skeleton project for a Kiji Rest Plugin.
see:
  www.kiji.org
  https://github.com/kijiproject/kiji-rest

It contains a source folder for an example plugin that sits on the endpoint /exampleplugin and
outputs "Hello World!"

To use this plugin put in into the lib/ folder of your kiji-rest installation($KIJI_HOME/rest/lib 
by default) and start KijiRest as normal.
