type
  # ProtocolError indicates we got something we don't understand. We might
  # even have lost framing, etc.. The connection should really be closed at this point.
  ProtocolError* = object of IOError