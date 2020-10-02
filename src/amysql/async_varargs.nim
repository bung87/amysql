import macros

macro asyncVarargs*(prc:untyped):untyped = 
  ## avoiding async proc can't take varargs
  result = nnkStmtList.newTree()
  var old = copyNimTree(prc)
  var vargs:NimNode
  var i = 0
  for it in old.params:
    if it.kind == nnkIdentDefs and it[1].kind == nnkBracketExpr and it[1][0].strVal == "varargs":
      break
    inc i
  vargs = old.params[i]
  var vargsId = vargs[0] # "args"
  vargs = nnkIdentDefs.newTree(
        vargsId,
        nnkBracketExpr.newTree(
          ident("seq"),
          vargs[1][1],
        ),
        newEmptyNode()
      )
  old.params[i] = vargs
  
  var vargsIdAsArg = nnkPrefix.newTree(
            ident("@"),
            vargsId
          )
  old.addPragma ident"async"
  old[0] = old.name # change to internal
  result.add old
  var paramsIds:seq[NimNode]
  for p in prc.params[1 ..< prc.params.len]:
    if p[0].strVal == vargsId.strVal:
      paramsIds.add vargsIdAsArg
    else:
      paramsIds.add p[0]
  var theCall = nnkCall.newTree(
        prc.name
        )
  for p in paramsIds:
    theCall.add p
  prc.body = nnkAsgn.newTree(
        ident("result"),
        theCall
      )
  result.add prc
 