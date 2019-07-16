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

abstract class Scala211VariableView(collectionSizeLimit: Int,
                                    stringSizeLimit: Int,
                                    blackList: List[String] = List(),
                                    lookIntoMethods: List[String] = List(),
                                    stopTypes: List[String] = Scala211VariableView.valTypes,
                                    changesOnly: Boolean = false) extends BaseVariableView(collectionSizeLimit, stringSizeLimit, blackList, lookIntoMethods) {

  private val ru = scala.reflect.runtime.universe
  private val mirror = ru.runtimeMirror(getClass.getClassLoader)
  private val stringsCache = mutable.Map[Any, String]()
  private val newStringsCache = mutable.Map[Any, String]()

  class ReferenceWrapper(val ref: AnyRef) {
    override def hashCode(): Int = ref.hashCode()

    override def equals(obj: Any): Boolean = {
      obj match {
        case value: ReferenceWrapper =>
          ref.eq(value.ref)
        case _ => false
      }
    }
  }

  private val refMap = mutable.Map[ReferenceWrapper, String]()
  private val refInvMap = mutable.Map[String, ReferenceWrapper]()

  def getRef(obj: Any, path: String): String = {
    obj match {
      case null => {
        if (refInvMap.contains(path)) {
          refMap.remove(refInvMap(path))
        }
        null
      }
      case ref: AnyRef =>
        val wrapper = new ReferenceWrapper(ref)
        if (refMap.contains(wrapper)) {
          refMap(wrapper)
        } else {
          if (refInvMap.contains(path)) {
            refMap.remove(refInvMap(path))
          }
          refMap(wrapper) = path
          refInvMap(path) = wrapper
          null
        }
      case _ => null
    }
  }

  def asString(obj: Any): String = {
    if (newStringsCache.contains(obj)) newStringsCache(obj) else {
      val str = obj match {
        case a: Array[_] => a.view.take(math.min(a.length, collectionSizeLimit)).mkString(",")
        case c: util.Collection[_] =>
          val it = c.iterator()
          val sb = new StringBuilder()
          var ind = 0
          while (it.hasNext && ind < collectionSizeLimit) {
            sb.append(it.next().toString)
            ind += 1
            if (it.hasNext && ind < collectionSizeLimit) sb.append(',')
          }
          sb.toString()
        case s: Seq[_] => s.view.take(math.min(s.length, collectionSizeLimit)).mkString(",")
        case e: Throwable =>
          val writer = new StringWriter()
          val out = new PrintWriter(writer)
          e.printStackTrace(out)
          writer.toString
        case _ => if (obj == null) null else obj.toString.take(stringSizeLimit)
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
    val result = new JSONObject()
    env.foreach {
      case (term, value) => getRef(value, term)
    }
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
        result.put(term, tree)
    }
    result
  }

  override def toJson(obj: Any, path: String, deep:  Int): JSONObject = {
    val data = Node(isAccessible = true, isLazy = false, obj, typeOfTerm(obj, path), path, getRef(obj, path))
    toJson1(data, deep)
  }

  def toJson1(objData: Node, deep: Int): JSONObject = {
    val root = new JSONObject()
    if (!stopHere(deep, objData)) {
      val instanceMirror = mirror.reflect(objData.value)
      val instanceSymbol = instanceMirror.symbol
      val members = instanceSymbol.toType.members
      val nodes = members.map {
        symbol =>
          (symbol, get(instanceMirror, symbol, objData.path))
      }
      nodes.foreach {
        case (symbol, data) =>
          if (data.isAccessible) {
            val tree = new JSONObject()
            if (data.ref == null) {
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
            } else {
              if (data.ref != data.path) {
                tree.put("ref", data.ref)
              } else {
                val subtree = toJson1(data, deep - 1)
                tree.put("value", if (subtree.isEmpty) asString(data.value) else subtree)
              }
            }
            root.put(symbol.name.toString.trim, tree)
          }
      }
    }
    root
  }

  case class Node(isAccessible: Boolean, isLazy: Boolean, value: Any, tpe: String, path: String, ref: String)

  val NO_ACCESS = Node(isAccessible = false, isLazy = false, null, null, null, null)

  def isMethodWithNoParams(method: ru.MethodSymbol): Boolean =
    method.paramLists.isEmpty || (method.paramLists.nonEmpty && method.paramLists.head.isEmpty)

  def get(instanceMirror: ru.InstanceMirror, symbol: ru.Symbol, path: String): Node = {
    if (symbol.isMethod && symbol.asMethod.isPublic) {
      val base = instanceMirror.symbol.baseClasses.map { x => x.fullName }
      val method = symbol.asMethod
      if (isMethodWithNoParams(method)) {
        val fullName = base.map { x => x + "." + symbol.name.toString }
        val intersection = lookIntoMethods.intersect(fullName)
        if (intersection.nonEmpty) {
          val m = instanceMirror.reflectMethod(method)
          val result = m.apply()
          val tpe = method.returnType.typeSymbol.fullName
          Node(isAccessible = true, isLazy = method.isLazy, result, tpe, s"$path.${method.name}", null)
        } else NO_ACCESS
      } else NO_ACCESS
    } else {
      if (symbol.isTerm && symbol.asTerm.getter.isPublic) {
        val term = symbol.asTerm
        try {
          val f = instanceMirror.reflectField(term)
          val fieldPath = s"$path.${term.name.toString.trim}"
          val value = f.get
          val tpe = term.typeSignature.toString
          Node(isAccessible = tpe != "<notype>", isLazy = term.isLazy, value, tpe, fieldPath, getRef(value, fieldPath))
        } catch {
          case _: Throwable => NO_ACCESS
        }
      } else NO_ACCESS
    }
  }

  def annotateTypes(): Boolean
}

object Scala211VariableView {
  val valTypes: List[String] = {
    val l = List("Int", "Long", "Byte", "Short", "Boolean", "Char", "Float", "Double")
    l.map{ x => "scala." + x} ++ l
  }
}