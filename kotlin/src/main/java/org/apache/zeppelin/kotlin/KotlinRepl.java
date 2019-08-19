/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.kotlin;

import org.jetbrains.kotlin.cli.common.repl.AggregatedReplStageState;
import org.jetbrains.kotlin.cli.common.repl.CompiledClassData;
import org.jetbrains.kotlin.cli.common.repl.ReplCodeLine;
import org.jetbrains.kotlin.cli.common.repl.ReplCompileResult;
import org.jetbrains.kotlin.cli.common.repl.ReplEvalResult;
import org.jetbrains.kotlin.scripting.compiler.plugin.impl.KJvmCompiledModuleInMemory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import kotlin.script.experimental.jvm.impl.KJvmCompiledScript;
import kotlin.script.experimental.jvmhost.repl.JvmReplCompiler;
import kotlin.script.experimental.jvmhost.repl.JvmReplEvaluator;
import org.apache.zeppelin.interpreter.InterpreterResult;

public class KotlinRepl {
  private static Logger logger = LoggerFactory.getLogger(KotlinRepl.class);

  private JvmReplCompiler compiler;
  private JvmReplEvaluator evaluator;
  private AggregatedReplStageState<?, ?> state;
  private AtomicInteger counter;
  private String outputDir;
  private int maxResult;

  public KotlinRepl(JvmReplCompiler compiler,
                    JvmReplEvaluator evaluator) {
    this(compiler, evaluator, null, 0);
  }

  @SuppressWarnings("unchecked")
  public KotlinRepl(JvmReplCompiler compiler,
                    JvmReplEvaluator evaluator,
                    String outputDir,
                    int maxResult) {
    this.compiler = compiler;
    this.evaluator = evaluator;
    ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    state = new AggregatedReplStageState(
        compiler.createState(stateLock),
        evaluator.createState(stateLock),
        stateLock);
    counter = new AtomicInteger(0);

    this.outputDir = outputDir;
    this.maxResult = maxResult;
  }

  public InterpreterResult eval(String code) {
    ReplCompileResult compileResult = compiler.compile(state,
        new ReplCodeLine(counter.getAndIncrement(), 0, code));

    if (compileResult instanceof ReplCompileResult.Incomplete) {
      return new InterpreterResult(InterpreterResult.Code.INCOMPLETE);
    }
    if (compileResult instanceof ReplCompileResult.Error) {
      ReplCompileResult.Error e = (ReplCompileResult.Error) compileResult;
      return new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage());
    }
    if (!(compileResult instanceof ReplCompileResult.CompiledClasses)) {
      return new InterpreterResult(InterpreterResult.Code.ERROR,
          "unknown compilation result:" + compileResult.toString());
    }

    ReplCompileResult.CompiledClasses classes =
        (ReplCompileResult.CompiledClasses) compileResult;
    writeClasses(classes);

    ReplEvalResult evalResult = evaluator.eval(state, classes, null, null);

    if (evalResult instanceof ReplEvalResult.Error) {
      ReplEvalResult.Error e = (ReplEvalResult.Error) evalResult;
      return new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage());
    }
    if (evalResult instanceof ReplEvalResult.Incomplete) {
      return new InterpreterResult(InterpreterResult.Code.INCOMPLETE);
    }
    if (evalResult instanceof ReplEvalResult.HistoryMismatch) {
      ReplEvalResult.HistoryMismatch e = (ReplEvalResult.HistoryMismatch) evalResult;
      return new InterpreterResult(
          InterpreterResult.Code.ERROR, "history mismatch at " + e.getLineNo());
    }
    if (evalResult instanceof ReplEvalResult.UnitResult) {
      return new InterpreterResult(InterpreterResult.Code.SUCCESS);
    }
    if (evalResult instanceof ReplEvalResult.ValueResult) {
      ReplEvalResult.ValueResult v = (ReplEvalResult.ValueResult) evalResult;
      String valueString = prepareValueString(v.getValue());
      return new InterpreterResult(
          InterpreterResult.Code.SUCCESS,
          v.getName() + ": " + v.getType() + " = " + valueString);
    }
    return new InterpreterResult(InterpreterResult.Code.ERROR,
        "unknown evaluation result: " + evalResult.toString());
  }

  private String prepareValueString(Object value) {
    if (value == null) {
      return "null";
    }
    if (!(value instanceof Collection<?>)) {
      return value.toString();
    }

    Collection<?> collection = (Collection<?>) value;

    if (collection.size() <= maxResult) {
      return value.toString();
    }

    return "[" + collection.stream()
        .limit(maxResult)
        .map(Object::toString)
        .collect(Collectors.joining(","))
        + " ... " + (collection.size() - maxResult) + " more]";
  }

  private void writeClasses(ReplCompileResult.CompiledClasses classes) {
    if (outputDir == null) {
      return;
    }

    for (CompiledClassData compiledClass: classes.getClasses()) {
      String filePath = compiledClass.getPath();
      if (filePath.contains("/")) {
        continue;
      }
      String classWritePath = outputDir + File.separator + filePath;
      writeClass(compiledClass.getBytes(), classWritePath);
    }

    // TODO(dk) refactor this nonsense
    try {
      ((KJvmCompiledModuleInMemory) ((KJvmCompiledScript<?>) classes.getData()).getCompiledModule())
          .getCompilerOutputFiles().forEach((name, bytes) -> {
            if (name.contains("class")) {
              writeClass(bytes, outputDir + File.separator + name);
            }
          });
    } catch (Exception e) {
      logger.info(e.getMessage());
    }
  }

  private void writeClass(byte[] classBytes, String path) {
    try (FileOutputStream fos = new FileOutputStream(path);
         OutputStream out = new BufferedOutputStream(fos)) {
      out.write(classBytes);
      out.flush();
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
  }
}
