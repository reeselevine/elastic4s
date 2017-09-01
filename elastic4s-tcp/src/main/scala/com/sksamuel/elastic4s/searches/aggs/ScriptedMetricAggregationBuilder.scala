package com.sksamuel.elastic4s.searches.aggs

import com.sksamuel.elastic4s.{FieldsMapper, ScriptBuilder}
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetricAggregationBuilder

import scala.collection.JavaConverters._

object ScriptedMetricAggregationBuilder {

  def apply(agg: ScriptedMetricAggregationDefinition): ScriptedMetricAggregationBuilder = {
    val builder = AggregationBuilders.scriptedMetric(agg.name)

    agg.initScript.map(ScriptBuilder.apply).foreach(builder.initScript)
    agg.combineScript.map(ScriptBuilder.apply).foreach(builder.combineScript)
    agg.mapScript.map(ScriptBuilder.apply).foreach(builder.mapScript)
    agg.reduceScript.map(ScriptBuilder.apply).foreach(builder.reduceScript)

    if (agg.params.nonEmpty) {
      builder.params(mapper(agg.params).asJava)
    }

    SubAggsFn(builder, agg.subaggs)
    if (agg.metadata.nonEmpty) builder.setMetaData(agg.metadata.asJava)
    builder
  }


  def mapper(m: Map[String, AnyRef]): Map[String, AnyRef] = {
    m map {
      case null => null
      case (name: String, nest: Map[_, _]) => name -> mapper(nest.asInstanceOf[Map[String, AnyRef]]).asJava
      case (name: String, seq: Seq[_]) => name -> seq.asJava
      case (name: String, a: AnyRef) => name -> a
    }
  }

}
