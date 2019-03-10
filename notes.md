# Custom mapping

enum fieldType
- string
- boolean
- decimal
- integer
- date
- object
- keyword

Field {
  name: String,
  type: FieldType,
  enabled: Boolean
}

Mapping(Version, Vec<Field>)

IndexManager(HashMap<Index, Mappings>)
  init(&self) -> impl Future<Item=()>>
