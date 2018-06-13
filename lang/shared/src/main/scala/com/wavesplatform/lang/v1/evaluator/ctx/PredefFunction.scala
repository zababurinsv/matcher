package com.wavesplatform.lang.v1.evaluator.ctx

import cats.data.EitherT
import com.wavesplatform.lang.v1.compiler.Types._
import com.wavesplatform.lang.TrampolinedExecResult
import com.wavesplatform.lang.v1.FunctionHeader
import com.wavesplatform.lang.v1.evaluator.ctx.PredefFunction.FunctionTypeSignature
import com.wavesplatform.lang.v1.compiler.TypeInferrer.inferResultType
import monix.eval.Coeval

trait PredefFunction {
  val name: String
  val args: List[(String, TYPEPLACEHOLDER)]
  val cost: Long
  val resultType: Map[TYPEPARAM, TYPE] => Either[String, TYPE]
  def eval(args: List[Any]): TrampolinedExecResult[Any]
  val signature: FunctionTypeSignature
  val header: FunctionHeader
}

object PredefFunction {

  case class FunctionTypeSignature(args: List[TYPEPLACEHOLDER], result: Map[TYPEPARAM, TYPE] => Either[String, TYPE], internalName: Short)

  case class PredefFunctionImpl(name: String,
                                cost: Long,
                                resultType: Map[TYPEPARAM, TYPE] => Either[String, TYPE],
                                args: List[(String, TYPEPLACEHOLDER)],
                                ev: List[Any] => Either[String, Any],
                                internalName: Short)
      extends PredefFunction {
    override def eval(args: List[Any]): TrampolinedExecResult[Any] = {
      EitherT.fromEither[Coeval](ev(args))
    }
    override val signature              = FunctionTypeSignature(args.map(_._2), resultType, internalName)
    override val header: FunctionHeader = FunctionHeader(internalName)
  }

  def apply(name: String, cost: Long, resultType: TYPEPLACEHOLDER, args: List[(String, TYPEPLACEHOLDER)], internalName: Short)(
      ev: List[Any] => Either[String, Any]): PredefFunction =
    PredefFunctionImpl(name, cost, (r => inferResultType(resultType, r)), args, ev, internalName)

  def create(name: String, cost: Long, resultType: Map[TYPEPARAM, TYPE] => Either[String, TYPE], args: List[(String, TYPEPLACEHOLDER)], internalName: Short)(
      ev: List[Any] => Either[String, Any]): PredefFunction =
    PredefFunctionImpl(name, cost, resultType, args, ev, internalName)

}
