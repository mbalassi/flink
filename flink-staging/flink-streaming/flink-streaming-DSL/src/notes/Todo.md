# General
## Macro
  - string -> get a streamn for source of create stream
  
# May 9

## Complete set of Parser

## Writing Test

## List of translation cases

    - create a schema -> Map(string, Schema)
          CreateSchema[T](s: String, schema: Schema, parentSchema: Option[String])

    - create a stream -> MAp (string-> (Schema, DataStream)
          CreateStream[T](name : String, schema : Schema, source: Option[Source[T]]) 
          
    - select : return a DataStream[OUT] / (WindowedDataStream must be flattened)
        +   projection -> [OUT] // typer
        
        +   StreamReference
            * RawStream : 
            * RawStream + Window + join
            * DerivedStream + window + join
            
            
            
       
