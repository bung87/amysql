import macros
import options

macro cachedProperty*(s: string, prc: untyped): untyped =
  if prc.kind notin {nnkProcDef, nnkLambda, nnkMethodDef, nnkDo}:
    error("Cannot transform this node kind into an cached_property proc." &
          " proc/method definition or lambda node expected.")

  let self = prc.params[1][0]
  let propName = ident(s.strVal)
  let prop =  nnkDotExpr.newTree(self, propName )
  let propIsNone = newCall("isNone", prop)
  let propGet = newCall("get", prop)
  var outerProcBody = nnkStmtList.newTree(
    nnkIfStmt.newTree(
    nnkElifBranch.newTree(propIsNone,
    nnkStmtList.newTree(
      nnkAsgn.newTree(
        prop,
        newCall("some", nnkPar.newTree prc.body)
      )
    )
    )
  ),
  nnkReturnStmt.newTree(propGet)
  )
  
  result = prc
  result.body = outerProcBody
  return result