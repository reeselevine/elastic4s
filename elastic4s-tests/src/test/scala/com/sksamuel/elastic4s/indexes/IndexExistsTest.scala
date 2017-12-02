package com.sksamuel.elastic4s.indexes

import com.sksamuel.elastic4s.http.ElasticDsl
import com.sksamuel.elastic4s.testkit.DiscoveryLocalNodeProvider
import org.scalatest.{Matchers, WordSpec}

import scala.util.Try

class IndexExistsTest extends WordSpec with Matchers with ElasticDsl with DiscoveryLocalNodeProvider {

  Try {
    http.execute {
      deleteIndex("indexexists")
    }.await
  }

  Try {
    http.execute {
      deleteIndex("indexexists2")
    }.await
  }

  http.execute {
    createIndex("indexexists").mappings {
      mapping("flowers") fields textField("name")
    }
  }.await

  http.execute {
    createIndex("indexexists2").mappings {
      mapping("flowers") fields textField("name")
    }
  }.await

  "an index exists request" should {
    "return true for an existing index" in {
      http.execute {
        indexExists("indexexists")
      }.await.isExists shouldBe true
    }
    "return true for two existing indexes" in {
      http.execute {
        indexesExist(Seq("indexexists", "indexexists2"))
      }.await.isExists shouldBe true
    }
    "return false for non existing index" in {
      http.execute {
        indexExists("qweqwewqe")
      }.await.isExists shouldBe false
    }
    "return false for one existing and on non-existing index" in {
      http.execute {
        indexesExist(Seq("indexexists", "qweqweqwe"))
      }.await.isExists shouldBe false
    }
  }
}
