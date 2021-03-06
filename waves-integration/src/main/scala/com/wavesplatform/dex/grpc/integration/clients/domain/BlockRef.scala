package com.wavesplatform.dex.grpc.integration.clients.domain

import com.wavesplatform.dex.domain.bytes.ByteStr

case class BlockRef(height: Int, id: ByteStr) {
  override def toString: String = s"BlockRef(h=$height, $id)"
}
