package astraea.spark.rasterframes

/**
 * Module utils.
 *
 * @author sfitch 
 * @since 9/7/17
 */
package object functions {
  private[rasterframes] def safeBinaryOp[T <: AnyRef, R >: T](op: (T, T) ⇒ R): ((T, T) ⇒ R) =
    (o1: T, o2: T) ⇒ {
      if (o1 == null) o2
      else if (o2 == null) o1
      else op(o1, o2)
    }
}
