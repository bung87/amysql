import json,tables,algorithm,sequtils
import ../conn

proc toSqlFormat(result: var string, node: JsonNode) =
  case node.kind
  of JObject:
    if node.fields.len > 0:
      result.add("{")
      var i = 0
      for key in toSeq(keys(node.fields)).sorted:
        if i > 0:
          result.add(",")
          result.add " "
        inc i
        escapeJson(key, result)
        result.add(": ")
        toSqlFormat(result, node.fields[key])
      result.add("}")
    else:
      result.add("{}")
  of JString:
    escapeJson(node.str, result)
  of JInt:
    when defined(js): result.add($node.num)
    else: result.addInt(node.num)
  of JFloat:
    # Fixme: implement new system.add ops for the JS target
    when defined(js): result.add($node.fnum)
    else: result.addFloat(node.fnum)
  of JBool:
    result.add(if node.bval: "true" else: "false")
  of JArray:
    if len(node.elems) != 0:
      result.add("[")
      for i in 0..len(node.elems)-1:
        if i > 0:
          result.add(",")
          result.add " "
        toSqlFormat(result, node.elems[i])
      result.add("]")
    else: result.add("[]")
  of JNull:
    result.add("null")

proc sqlFormat*(conn: Connection, node: JsonNode): string =
  ## Returns a JSON Representation of `node`, with spaces and
  ## on single line.

  result = ""
  if conn.isMaria:
    result = $node
  else:
    toSqlFormat(result, node)