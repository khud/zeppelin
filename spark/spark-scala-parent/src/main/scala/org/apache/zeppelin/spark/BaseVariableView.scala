package org.apache.zeppelin.spark

import org.json.JSONObject

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

abstract class BaseVariableView(val arrayLimit: Int,
                                val stringLimit: Int,
                                val blackList: List[String],
                                val expandMethods: List[String]) extends VariableView {

  protected val blackListAsSet: Set[String] = blackList.toSet

  def variables(): List[String]

  def valueOfTerm(id: String): Option[Any]

  def typeOfTerm(obj: Any, id: String): String

  override def toJson(): String =
    toJson(variables().filter { x => !blackListAsSet.contains(x) }.map { name =>
      (name, valueOfTerm(name).orNull)
    }.toMap).toString(2)


  def toJson(env: Map[String, Any]): JSONObject

  def toJson(obj: Any, path: String): JSONObject = {
    toJson(obj, path, 3)
  }

  def toJson(obj: Any,
             path: String,
             deep: Int): JSONObject
}