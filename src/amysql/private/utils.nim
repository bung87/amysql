import macros

macro cachedProperty*(s: string, prc: untyped): untyped =
  if prc.kind notin {nnkProcDef, nnkLambda, nnkMethodDef, nnkDo}:
    error("Cannot transform this node kind into an cached_property proc." &
          " proc/method definition or lambda node expected.")
  let self = prc.params[1][0]
  var outerProcBody = nnkStmtList.newTree(
      nnkIfStmt.newTree(
        nnkElifBranch.newTree(
          nnkDotExpr.newTree(
            self,
            newIdentNode(s.strVal)
    ),
    nnkStmtList.newTree(
      nnkAsgn.newTree(
        newIdentNode("result"),
        nnkDotExpr.newTree(
          self,
          newIdentNode(s.strVal)
      )
    )
    )
  ),
        nnkElse.newTree(
          nnkStmtList.newTree(
            nnkAsgn.newTree(
              nnkDotExpr.newTree(
                self,
                newIdentNode(s.strVal)
    ),
    prc.body
  ),
            nnkAsgn.newTree(
              newIdentNode("result"),
              nnkDotExpr.newTree(
                self,
                newIdentNode(s.strVal)
    )
  )
    )
  )
    )
  )
  result = prc
  result.body = outerProcBody
  return result