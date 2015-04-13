package almanac.model

sealed trait Criteria {
  val operator: String = getClass.getSimpleName.toUpperCase
  def and(that: Criteria):And = Criteria.all(this, that)
  def or(that: Criteria):Or = Criteria.any(this, that)
}

object Criteria {
  private[model] case class FactCriteriaBuilder(fact: String) {
    def in(ref: String*) = In(fact, ref toSet)
    def notIn(ref: String*) = NotIn(fact, ref toSet)
    def is(ref: String) = Is(fact, ref)
    def isNot(ref: String) = IsNot(fact, ref)
    def like(regex: String) = Like(fact, regex)
  }

  def fact(fact: String) = FactCriteriaBuilder(fact)
  def all(others: Criteria*) = (others foldLeft And())(_ and _)
  def any(others: Criteria*) = (others foldLeft Or()) (_ or _)
}

object NonCriteria extends Criteria

sealed trait FactCriteria[R] extends Criteria {
  val reference: R
  val fact: String
  override def toString: String = "%s %s %s".format(fact, operator, reference)
}

case class In   (fact: String, reference: Set[String]) extends FactCriteria[Set[String]]
case class NotIn(fact: String, reference: Set[String]) extends FactCriteria[Set[String]]
case class Is   (fact: String, reference: String     ) extends FactCriteria[String]
case class IsNot(fact: String, reference: String     ) extends FactCriteria[String]
case class Like (fact: String, reference: String     ) extends FactCriteria[String]

sealed trait CollectiveCriteria extends Iterable[Criteria] with Criteria {
  protected val criteria: Set[Criteria]
  override def iterator: Iterator[Criteria] = criteria.iterator

  override def toString: String = "(   " + (
    this.criteria map (_.toString.replace("\n", "\n    ")) mkString ("\n" + operator.padTo(4, ' '))
  ) + "\n)"
}

case class And private[model] (criteria: Set[Criteria] = Set()) extends CollectiveCriteria {
  override def and(that: Criteria): And = that match {
    case NonCriteria => this
    case otherAll: And => And(this.criteria ++ otherAll.criteria)
    case otherCriterion => And(this.criteria + otherCriterion)
  }
}

case class Or private[model] (criteria: Set[Criteria] = Set()) extends CollectiveCriteria {
  override def or(that: Criteria): Or = that match {
    case NonCriteria => this
    case otherAny: Or => Or(this.criteria ++ otherAny.criteria)
    case otherCriterion => Or(this.criteria + otherCriterion)
  }
}