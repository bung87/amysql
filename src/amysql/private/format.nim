when (NimMajor, NimMinor) >= (2, 0):
  import db_connector/db_common
else:
  import db_common
import ./quote

proc dbFormat*(formatstr: SqlQuery, args: varargs[string] | seq[string]): string =
  result = ""
  var a = 0
  for c in items(string(formatstr)):
    if c == '?':
      add(result, dbQuote(args[a]))
      inc(a)
    else:
      add(result, c)