#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# BiJuTy -- Apache Spark environment template
# ============================================
#
# Sourced by Spark before the master/worker daemons start.
# Values written as FRAMEWORK_* placeholders are filled in by BiJuTy from the
# Configuration Panel; the remaining values can be edited by hand.
#
# Reference: https://spark.apache.org/docs/4.1.3/spark-standalone.html

# --- Master / driver ---
export SPARK_MASTER_HOST="FRAMEWORK_MASTER_NODE"
export SPARK_MASTER_PORT="7077"
export SPARK_DRIVER_MEMORY="FRAMEWORK_MEM_MASTER"

# --- Worker / executor ---
export SPARK_WORKER_CORES="FRAMEWORK_PARALLELISM_PER_WORKER"
export SPARK_WORKER_MEMORY="FRAMEWORK_MEM_PER_WORKER"
export SPARK_EXECUTOR_CORES="FRAMEWORK_PARALLELISM_PER_WORKER"
export SPARK_EXECUTOR_MEMORY="FRAMEWORK_MEM_PER_WORKER"

# --- Directories ---
export SPARK_CONF_DIR="FRAMEWORK_CONF_DIR"
export SPARK_WORKER_DIR="FRAMEWORK_LOCAL_DIR/worker"
export SPARK_LOCAL_DIRS="FRAMEWORK_LOCAL_DIR/local"
export SPARK_LOG_DIR="FRAMEWORK_LOG_DIR"
export SPARK_PID_DIR="FRAMEWORK_PID_DIR"

# --- Java runtime (Spark 4 requires Java 17 or 21) ---
export JAVA_HOME="FRAMEWORK_JAVA_HOME"
