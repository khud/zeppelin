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

import scala.collection.mutable

abstract class Scala211VariableView(arrayLimit: Int,
                                    stringLimit: Int,
                                    blackList: List[String] = List(),
                                    expandMethods: List[String] = List(),
                                    stopTypes: List[String] = List("Int", "Long", "Double", "String"),
                                    changesOnly: Boolean = false) extends BaseVariableView(arrayLimit, stringLimit, blackList, expandMethods) {

  private val ru = scala.reflect.runtime.universe
  private val mirror = ru.runtimeMirror(getClass.getClassLoader)
  private val stringsCache = mutable.Map[Any, String]()
  private val newStringsCache = mutable.Map[Any, String]()

  def asString(obj: Any): String = {
    if (newStringsCache.contains(obj)) newStringsCache(obj) else {
      val str = obj match {
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
          val writer = new StringWriter()
          val out = new PrintWriter(writer)
          e.printStackTrace(out)
          writer.toString
        case _ => if (obj == null) null else obj.toString.take(stringLimit)
      }
      newStringsCache.put(obj, str)
      str
    }
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
    changedTerms.clear()

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
        val tpe = typeOfTerm(value, term)
        tree.put("type", tpe)
        val changed = isChanged(Node(isAccessible = true, isLazy = false, value, tpe, term))
        if (changed) markChanged(term)
        result.put(term, tree)
    }

    mergeEnv()
    filter(result)
  }

  def mergeEnv(): Unit = {
    env ++= newEnv
    newEnv.clear()
    stringsCache ++= newStringsCache
    newStringsCache.clear()
  }

  def filter(json: JSONObject): JSONObject = {
    if (changesOnly) {
      val result = new JSONObject()
      val it = json.keys()
      while (it.hasNext) {
        val key = it.next()
        if (changedTerms.contains(key)) result.put(key, json.get(key))
      }
      result
    } else json
  }

  override def toJson(obj: Any, path: String, deep:  Int): JSONObject = {
    val data = Node(isAccessible = true, isLazy = false, obj, typeOfTerm(obj, path), path)
    toJson1(data, deep)._1
  }

  def toJson1(objData: Node, deep: Int): (JSONObject, Boolean) = {
    val root = new JSONObject()
    val refChanged = isChanged(objData)
    var objChanged = refChanged
    if (stopHere(deep, objData)) {
      (root, refChanged)
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
              val changed = isChanged(data)
              if (changed) tree.put("changed", changed)
            } else {
              val (subtree, memberChanged) = toJson1(data, deep - 1)
              objChanged |= memberChanged
              tree.put("value", if (subtree.isEmpty) asString(data.value) else subtree)
            }
            root.put(symbol.name.toString.trim, tree)
          }
      }
      if (objChanged) markChanged(objData.path)
      (root, refChanged)
    }
  }

  def markChanged(path: String): Unit = {
    val p = path.indexOf('.')
    val head = if (p >= 0) path.substring(0, p) else path
    changedTerms.add(head)
  }

  private val changedTerms = mutable.Set[String]()
  private val env = mutable.Map[String, Any]()
  private val newEnv = mutable.Map[String, Any]()

  private val valTypes: Set[String] = {
    val l = List("Int", "Long", "Byte", "Short", "Boolean", "Char", "Float", "Double")
    (l.map{ x => "scala." + x} ++ l).toSet
  }

  def isChanged(node: Node): Boolean  = {
    if (!env.contains(node.path)) {
      newEnv(node.path) = node.value
      true
    } else {
      val oldValue = env(node.path)
      newEnv(node.path) = node.value
      node.value match {
        case null => oldValue != null
        case _: Array[_] | _: util.Collection[_] | _: Seq[_] =>
          !asString(node.value).equals(stringsCache(oldValue))
        case ref: AnyRef => if (!valTypes.contains(node.tpe)) {
          oldValue.isInstanceOf[AnyRef] && !oldValue.asInstanceOf[AnyRef].eq(ref)
        } else !oldValue.equals(node.value)
      }
    }
  }

  case class Node(isAccessible: Boolean, isLazy: Boolean, value: Any, tpe: String, path: String)

  val NO_ACCESS = Node(isAccessible = false, isLazy = false, null, null, null)

  def get(instanceMirror: ru.InstanceMirror, symbol: ru.Symbol, path: String): Node = {
    if (symbol.isMethod && symbol.asMethod.isPublic) {
      val base = instanceMirror.symbol.baseClasses.map { x => x.fullName }
      val method = symbol.asMethod
      if (method.paramLists.isEmpty || (method.paramLists.nonEmpty && method.paramLists.head.isEmpty)) {
        val fullName = base.map { x => x + "." + symbol.name.toString }
        val intersection = expandMethods.intersect(fullName)
        if (intersection.nonEmpty) {
          val f = instanceMirror.reflectMethod(method)
          val result = f.apply()
          val tpe = method.returnType.typeSymbol.fullName
          Node(isAccessible = true, isLazy = method.isLazy, result, tpe, s"$path.${method.name}")
        } else NO_ACCESS
      } else NO_ACCESS
    } else {
      if (symbol.isTerm && symbol.asTerm.getter.isPublic) {
        val term = symbol.asTerm
        val f = try {
          instanceMirror.reflectField(term)
        } catch {
          case _: Throwable => null
        }
        val fieldPath = s"$path.${term.name.toString.trim}"
        if (f == null)
          NO_ACCESS
        else {
          val value = f.get
          try {
            val tpe = term.typeSignature.toString
            Node(isAccessible = tpe != "<notype>", isLazy = term.isLazy, value, tpe, fieldPath)
          } catch {
            case _: Throwable => NO_ACCESS
          }
        }
      } else NO_ACCESS
    }
  }

  def annotateTypes(): Boolean
}