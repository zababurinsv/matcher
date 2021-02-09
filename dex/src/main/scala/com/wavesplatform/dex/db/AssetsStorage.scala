package com.wavesplatform.dex.db

import cats.syntax.applicative._
import cats.syntax.functor._
import cats.{Applicative, Functor}
import com.wavesplatform.dex.db.leveldb.LevelDb
import com.wavesplatform.dex.domain.asset.Asset
import com.wavesplatform.dex.domain.asset.Asset.IssuedAsset
import com.wavesplatform.dex.grpc.integration.dto.BriefAssetDescription

import java.util.concurrent.ConcurrentHashMap

trait AssetsStorage[F[_]] {
  def put(asset: IssuedAsset, item: BriefAssetDescription): F[Unit]
  def get(asset: IssuedAsset): F[Option[BriefAssetDescription]]
}

object AssetsStorage {

  def cache[F[_]: Applicative](inner: AssetsStorage[F]): AssetsStorage[F] = new AssetsStorage[F] {

    private val assetsCache = new ConcurrentHashMap[Asset, BriefAssetDescription]

    // We don't check assetCache here before put, because it is a responsibility of a user.
    def put(asset: Asset.IssuedAsset, item: BriefAssetDescription): F[Unit] =
      inner.put(asset, item).map { r =>
        assetsCache.putIfAbsent(asset, item)
        r
      }

    def get(asset: Asset.IssuedAsset): F[Option[BriefAssetDescription]] =
      Option(assetsCache.get(asset)) match {
        case None =>
        case x =>
      }
      Option {
        assetsCache.computeIfAbsent(asset, inner.get(_).orNull)
      }

  }

  def levelDB[F[_]](levelDb: LevelDb[F]): AssetsStorage[F] = new AssetsStorage[F] {
    def put(asset: IssuedAsset, record: BriefAssetDescription): F[Unit] = levelDb.readWrite(_.put(DbKeys.asset(asset), Some(record)))
    def get(asset: IssuedAsset): F[Option[BriefAssetDescription]] = levelDb.readOnly(_.get(DbKeys.asset(asset)))
  }

  def inMem[F[_]: Applicative]: AssetsStorage[F] = new AssetsStorage[F] {
    private val assetsCache = new ConcurrentHashMap[Asset, BriefAssetDescription]
    def put(asset: IssuedAsset, item: BriefAssetDescription): F[Unit] = assetsCache.putIfAbsent(asset, item).pure[F].as(())
    def get(asset: IssuedAsset): F[Option[BriefAssetDescription]] = Option(assetsCache get asset).pure[F]
  }

  implicit final class Ops[F[_]: Functor](val self: AssetsStorage[F]) extends AnyVal {
    def contains(asset: IssuedAsset): F[Boolean] = self.get(asset).map(_.nonEmpty)
    def get(asset: Asset): Option[BriefAssetDescription] = asset.fold(BriefAssetDescription.someWavesDescription)(self.get)

    def unsafeGet(asset: Asset): BriefAssetDescription =
      get(asset).getOrElse(throw new RuntimeException(s"Unknown asset: ${asset.toString}"))

    def unsafeGetDecimals(asset: Asset): Int = unsafeGet(asset).decimals
    def unsafeGetHasScript(asset: Asset): Boolean = unsafeGet(asset).hasScript
  }

}
