
object StagesNameEnum extends Enumeration {

  type StageType = Value

  val Input, DataSet, Extractor, Transformer, DataJoin, DataAggregation, DataOutputWriter = Value
}
