import scala.language.implicitConversions

trait FilterCond {
  def eval(r: Row): Option[Boolean]
}

case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    r.get(colName).map(predicate)
  }
}

case class Compound(op: (Boolean, Boolean) => Boolean, conditions: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    // Evaluăm fiecare condiție și colectăm rezultatele într-o listă
    val results = conditions.map(_.eval(r))

    // Dacă există cel puțin o condiție care nu poate fi evaluată, returnăm None
    if (results.exists(_.isEmpty)) {
      None
    } else {
      // Extragem rezultatele evaluate și aplicăm operația op între ele
      val booleanResults = results.flatten
      Some(booleanResults.reduce(op))
    }
  }
}

case class Not(f: FilterCond) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    f.eval(r).map(!_)
  }
}

def And(f1: FilterCond, f2: FilterCond): FilterCond = {
  Compound(_ && _, List(f1, f2))
}
def Or(f1: FilterCond, f2: FilterCond): FilterCond = {
  Compound(_ || _, List(f1, f2))
}
def Equal(f1: FilterCond, f2: FilterCond): FilterCond = {
  Compound(_ == _, List(f1, f2))
}

case class Any(fs: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    // Evaluăm fiecare condiție și verificăm dacă cel puțin una este satisfăcută
    val results = fs.map(_.eval(r))

    // Dacă nu există nicio condiție care să fie satisfăcută, returnăm None
    if (results.isEmpty || results.forall(_.contains(false))) {
      Some(false)
    } else {
      Some(true)
    }
  }
}

case class All(fs: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    // Evaluăm fiecare condiție și verificăm dacă toate sunt satisfăcute
    val results = fs.map(_.eval(r))

    // Dacă există cel puțin o condiție care nu este satisfăcută, returnăm false
    if (results.isEmpty || results.exists(_.contains(false))) {
      Some(false)
    } else {
      Some(true)
    }
  }

}

implicit def tuple2Field(t: (String, String => Boolean)): Field = Field(t._1, t._2)

extension (f: FilterCond) {
  def ===(other: FilterCond) = Equal(f, other)
  def &&(other: FilterCond) = And(f, other)
  def ||(other: FilterCond) = Or(f, other)
  def !! = Not(f)
}