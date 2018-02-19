package astraea.spark.rasterframes.rules

import org.apache.spark.sql.catalyst.expressions.ScalaUDF
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.locationtech.geomesa.spark.jts.rules.GeometryLiteral

/**
 *
 * @since 2/19/18
 */
object SpatialUDFSubstitutionRules extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {

    plan.transform {
      case q: LogicalPlan ⇒ q.transformExpressions {
        case s: ScalaUDF ⇒
          println("-------------> " + s)
          s
        case g: GeometryLiteral ⇒
          println("+++++++++++++> " + g)
          g
      }
    }
  }
}
