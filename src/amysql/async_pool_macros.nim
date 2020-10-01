import macros
import ./async_pool

macro asyncPooled*(prc:untyped):untyped =
  let tyAsyncPool = bindSym"AsyncPool" 
  let oldBody = prc.body
  result = prc
  let conIdx = ident"conIdx"
  let conn = prc.params[1][0]
  let getConn = nnkWhenStmt.newTree(
    nnkElifBranch.newTree(
      nnkInfix.newTree(
        newIdentNode("is"),
        conn,
        tyAsyncPool
      ),
      nnkStmtList.newTree(
        nnkLetSection.newTree(
          nnkIdentDefs.newTree(
            conIdx,
            newEmptyNode(),
            nnkCommand.newTree(
              newIdentNode("await"),
              nnkCall.newTree(
                nnkDotExpr.newTree(
                  conn,
                  newIdentNode("getFreeConnIdx")
                )
              )
            )
          )
        ),
        nnkLetSection.newTree(
          nnkIdentDefs.newTree(
            conn,
            newEmptyNode(),
            nnkBracketExpr.newTree(
              nnkDotExpr.newTree(
                conn,
                newIdentNode("conns")
              ),
              conIdx
            )
          )
        )
      )
    )
  )
  result.body = nnkStmtList.newTree(
    getConn,
    oldBody
  )
  result.params[1] = nnkIdentDefs.newTree(
    conn,
    nnkInfix.newTree(
    newIdentNode("|"),
    prc.params[1][1],
    tyAsyncPool
  ),
    newEmptyNode()
  )
