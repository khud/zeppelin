package scala.tools.nsc.interpreter

import scala.reflect.runtime.{universe => ru}

class FixedValueOfTermFromScala2_12(iloop: ILoop) extends IMain(iloop.settings) {
  import global._

  import scala.util.{Try => Trying}

  private lazy val importToGlobal = global mkImporter ru
  private lazy val importToRuntime = ru.internal createImporter global

  private implicit def importFromRu(sym: ru.Symbol): Symbol = importToGlobal importSymbol sym
  private implicit def importToRu(sym: Symbol): ru.Symbol   = importToRuntime importSymbol sym

  override lazy val formatting = new Formatting(4) {
    def prompt = iloop.prompt
  }
  override protected def parentClassLoader =
    settings.explicitParentLoader.getOrElse( classOf[ILoop].getClassLoader )

  def oldValueOf(id: String) = super.valueOfTerm(id)

  // see https://github.com/scala/scala/pull/5852/commits/a9424205121f450dea2fe2aa281dd400a579a2b7
  override def valueOfTerm(id: String): Option[Any] = exitingTyper {
    def fixClassBasedFullName(fullName: List[String]): List[String] = {
      if (settings.Yreplclassbased.value) {
        val line :: read :: rest = fullName
        line :: read :: "INSTANCE" :: rest
      } else fullName
    }
    def value(fullName: String) = {
      import runtimeMirror.universe.{ Symbol, InstanceMirror, TermName }
      val pkg :: rest = fixClassBasedFullName((fullName split '.').toList)
      val top = runtimeMirror.staticPackage(pkg)
      @annotation.tailrec
      def loop(inst: InstanceMirror, cur: Symbol, path: List[String]): Option[Any] = {
        def mirrored =
          if (inst != null) inst
          else runtimeMirror reflect (runtimeMirror reflectModule cur.asModule).instance

        path match {
          case last :: Nil  =>
            cur.typeSignature.decls find (x => x.name.toString == last && x.isAccessor) map { m =>
              (mirrored reflectMethod m.asMethod).apply()
            }
          case next :: rest =>
            val s = cur.typeSignature.member(TermName(next))
            val i =
              if (s.isModule) {
                if (inst == null) null
                else runtimeMirror reflect (inst reflectModule s.asModule).instance
              }
              else if (s.isAccessor) {
                runtimeMirror reflect (mirrored reflectMethod s.asMethod).apply()
              }
              else {
                assert(false, s.fullName)
                inst
              }
            loop(i, s, rest)
          case Nil => None
        }
      }
      loop(null, top, rest)
    }
    Option(symbolOfTerm(id)) filter (_.exists) flatMap (s => Trying(value(s.fullName)).toOption.flatten)
  }
}