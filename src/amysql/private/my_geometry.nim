import endians

type 
  MyGeometry* = ref MyGeometryObj
  MyGeometryObj = object
    data: string

proc fromWkb*(srid:int , wkb: sink string): MyGeometry =
  var bytes = newString(wkb.len + 4)
  littleEndian32(bytes[0].addr, srid.unSafeAddr)
  copyMem(bytes[4].addr, wkb.unSafeAddr, wkb.len)
  MyGeometry(data: bytes)

proc newMyGeometry*(data: sink string): MyGeometry =
  MyGeometry(data: data)

proc srid*(self: MyGeometry): int = 
  when system.cpuEndian == bigEndian:
    swapEndian32(result.addr, self.data.addr)
  else:
    copyMem(result.addr, self.data.addr, 4)

proc wkb*(self: MyGeometry): string = 
  self.data[4 .. self.data.high]

proc data*(self: MyGeometry):lent string = 
  self.data

proc `==`*(a,b: MyGeometry): bool = a.data == b.data

when isMainModule:
  proc buildString(data:openarray[SomeInteger]): string = 
    let dataLen = data.len
    result = newString(data.len)
    for i in 0 ..< dataLen:
      result[i] = data[i].char
  let data1 = buildString [0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63]
  assert newMyGeometry(data1) == newMyGeometry(data1)