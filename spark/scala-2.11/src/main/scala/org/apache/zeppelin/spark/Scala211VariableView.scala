package org.apache.zeppelin.spark

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

import java.io.{PrintWriter, StringWriter}
import java.util

import org.json._

abstract class Scala211VariableView(arrayLimit: Int,
                                    stringLimit: Int,
                                    blackList: List[String] = List(),
                                    expandMethods: List[String] = List(),
                                    stopTypes: List[String] = List("Int", "Long", "Double", "String")) extends BaseVariableView(arrayLimit, stringLimit, blackList, expandMethods) {

  private val ru = scala.reflect.runtime.universe
  private val mirror = ru.runtimeMirror(getClass.getClassLoader)

  def asString(obj: Any): String = obj match {
    case a: Array[_] => a.view.take(math.min(a.length, arrayLimit)).mkString(",")
    case c: util.Collection[_] =>
      val it = c.iterator()
      val sb = new StringBuilder()
      var ind = 0
      while (it.hasNext && ind < arrayLimit) {
        sb.append(it.next().toString)
        ind += 1
        if (it.hasNext && ind < arrayLimit) sb.append(',')
      }
      sb.toString()
    case s: Seq[_] => s.view.take(math.min(s.length, arrayLimit)).mkString(",")
    case e: Throwable =>
      val out = new PrintWriter(new StringWriter())
      e.printStackTrace(out)
      out.toString
    case _ => if (obj == null) null else obj.toString.take(stringLimit)
  }

  def length(obj: Any): Int = obj match {
    case a: Array[_] => a.length
    case c: util.Collection[_] => c.size
    case q: Seq[_] => q.length
    case _ => -1
  }

  def isStopType(tpe: String): Boolean = {
    tpe != null && stopTypes.exists { x => tpe.matches(x) }
  }

  def stopHere(deep: Int, data: Node): Boolean = {
    if (data.value == null) true else {
      val className = data.value.getClass.getCanonicalName
      if (className == null ||
        className.startsWith("java.") ||
        className.startsWith("scala.")) true
      else {
        if (deep == 0) {
          true
        } else {
          if (data.tpe == "<notype>" || isStopType(data.tpe)) true else false
        }
      }
    }
  }

  override def toJson(env: Map[String, Any]): JSONObject = {
    val result = new JSONObject()
    env.foreach {
      case (term, value) =>
        val tree = new JSONObject()
        val len = length(value)
        if (len >= 0) {
          tree.put("length", len)
          tree.put("value", asString(value))
        } else {
          val json = toJson(value, term)
          if (json.isEmpty)
            tree.put("value", asString(value))
          else
            tree.put("value", json)
        }
        tree.put("type", typeOfTerm(value, term))
        result.put(term, tree)
    }
    result
  }

  override def toJson(obj: Any, path: String, deep:  Int): JSONObject = {
    val data = Node(isAccessible = true, isLazy = false, obj, typeOfTerm(obj, path), path)
    toJson1(data, deep)
  }

  def toJson1(objData: Node, deep: Int): JSONObject = {
    val root = new JSONObject()
    if (stopHere(deep, objData)) {
      root
    } else {
      val instanceMirror = mirror.reflect(objData.value)
      val instanceSymbol = instanceMirror.symbol
      val members = instanceSymbol.toType.members
      members.foreach {
        symbol =>
          val data = get(instanceMirror, symbol, objData.path)
          if (data.isAccessible) {
            val tree = new JSONObject()
            tree.put("type", data.tpe)
            if (data.isLazy) tree.put("lazy", data.isLazy)
            val len = length(data.value)
            if (len >= 0) {
              tree.put("length", len)
              tree.put("value", asString(data.value))
            } else {
              val subtree = toJson1(data, deep - 1)
              tree.put("value", if (subtree.isEmpty) asString(data.value) else subtree)
            }
            root.put(symbol.name.toString.trim, tree)
          }
      }
      root
    }
  }

  case class Node(isAccessible: Boolean, isLazy: Boolean, value: Any, tpe: String, path: String)

  val NO_ACCESS = Node(isAccessible = false, isLazy = false, null, null, null)

  // FIXME: refactor it
  def get(instanceMirror: ru.InstanceMirror, symbol: ru.Symbol, path: String): Node = {
    if (symbol.isTerm && !symbol.asTerm.getter.isPublic) NO_ACCESS else
    if (symbol.isClass) {
      NO_ACCESS
    } else if (symbol.isMethod) {
      val base = instanceMirror.symbol.baseClasses.map { x => x.fullName }
      if (symbol.asMethod.paramLists.nonEmpty && symbol.asMethod.paramLists.head.isEmpty) {
        val fullName = base.map { x => x + "." + symbol.name.toString }
        val intersection = expandMethods.intersect(fullName)
        if (intersection.nonEmpty) {
          val f = instanceMirror.reflectMethod(symbol.asMethod)
          val result = f.apply()
          val tpe = symbol.asMethod.returnType.typeSymbol.fullName
          Node(isAccessible = true, isLazy = symbol.asTerm.isLazy, result, tpe, s"$path.${symbol.asTerm.name}")
        } else NO_ACCESS
      } else NO_ACCESS
    } else if (symbol.isTerm) {
      val f = try {
        instanceMirror.reflectField(symbol.asTerm)
      } catch {
        case _: Throwable => null
      }
      val fieldPath = s"$path.${symbol.asTerm.name.toString.trim}"
      if (f == null)
        NO_ACCESS
      else {
        val value = f.get
        val tpe = symbol.asTerm.typeSignature.toString
        Node(isAccessible = tpe != "<notype>", isLazy = symbol.asTerm.isLazy, value, tpe, fieldPath)
      }
    } else NO_ACCESS
  }

  def annotateTypes(): Boolean
}