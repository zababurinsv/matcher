package com.wavesplatform.dex.api.http

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import com.wavesplatform.dex.actors.OrderBookAskAdapter
import com.wavesplatform.dex.actors.orderbook.AggregatedOrderBookActor.Depth
import com.wavesplatform.dex.actors.orderbook.OrderBookActor.MarketStatus
import com.wavesplatform.dex.api.http.entities.MatcherResponse.toHttpResponse
import com.wavesplatform.dex.api.http.entities.{HttpOrderBook, HttpOrderBookStatus, OrderBookUnavailable, SimpleResponse}
import com.wavesplatform.dex.domain.asset.{Asset, AssetPair}
import com.wavesplatform.dex.model.MatcherModel.{DecimalsFormat, Denormalized}
import com.wavesplatform.dex.time.Time

import scala.concurrent.{ExecutionContext, Future}

class OrderBookHttpInfo(
  settings: OrderBookHttpInfo.Settings,
  askAdapter: OrderBookAskAdapter,
  time: Time,
  assetDecimals: Asset => Future[Option[Int]]
)(
  implicit ec: ExecutionContext
) {

  private val emptyMarketStatus = toHttpMarketStatusResponse(MarketStatus(None, None, None))

  def getMarketStatus(assetPair: AssetPair): Future[HttpResponse] =
    askAdapter.getMarketStatus(assetPair).map {
      case Left(e) => toHttpResponse(OrderBookUnavailable(e))
      case Right(maybeMarketStatus) =>
        maybeMarketStatus match {
          case Some(ms) => toHttpMarketStatusResponse(ms)
          case None => emptyMarketStatus
        }
    }

  private def toHttpMarketStatusResponse(ms: MarketStatus): HttpResponse = toHttpResponse(SimpleResponse(HttpOrderBookStatus fromMarketStatus ms))

  def getHttpView(assetPair: AssetPair, format: DecimalsFormat, depth: Option[Depth]): Future[HttpResponse] =
    askAdapter.getHttpView(assetPair, format, settings.nearestBigger(depth)).flatMap {
      case Right(Some(x)) => Future.successful(x)
      case Right(None) => getDefaultHttpView(assetPair, format)
      case Left(e) => Future.successful(toHttpResponse(OrderBookUnavailable(e)))
    }

  private def getDefaultHttpView(assetPair: AssetPair, format: DecimalsFormat): Future[HttpResponse] =
    assetPairDecimals(assetPair, format).map { assetPairDecimals =>
      val entity = HttpOrderBook(time.correctedTime(), assetPair, Seq.empty, Seq.empty, assetPairDecimals)
      HttpResponse(
        entity = HttpEntity(
          ContentTypes.`application/json`,
          HttpOrderBook.toJson(entity)
        )
      )
    }

  private def assetPairDecimals(assetPair: AssetPair, format: DecimalsFormat): Future[Option[(Int, Int)]] = format match {
    case Denormalized => assetDecimals(assetPair.amountAsset).zip(assetDecimals(assetPair.priceAsset)).map(x => x._1.zip(x._2))
    case _ => Future.successful(None)
  }

}

object OrderBookHttpInfo {

  case class Settings(depthRanges: List[Int], defaultDepth: Option[Int]) {

    def nearestBigger(to: Option[Int]): Int =
      to.orElse(defaultDepth)
        .flatMap(desiredDepth => depthRanges.find(_ >= desiredDepth))
        .getOrElse(depthRanges.max)

  }

}
