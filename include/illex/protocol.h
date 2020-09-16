#pragma once

#include <variant>

#include "illex/raw_protocol.h"
#include "illex/zmq_protocol.h"

namespace illex {

using StreamProtocol = std::variant<RawProtocol, ZMQProtocol>;

}