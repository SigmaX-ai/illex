// Copyright 2020 Delft University of Technology
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "illex/log.h"
#include "illex/cli.h"
#include "illex/file.h"
#include "illex/zmq_server.h"
#include "illex/arrow.h"
#include "illex/status.h"

namespace illex {

/// @brief Common options for all subcommands
static void AddCommonOpts(CLI::App *sub, ProductionOptions *prod, std::string *schema_file) {
  sub->add_option("i,-i,--input",
                  *schema_file,
                  "An Arrow schema to generate the JSON from.")->required()->check(CLI::ExistingFile);
  sub->add_option("-m,--num-jsons", prod->num_jsons, "Number of JSONs to send (default=1).");
  sub->add_option("-s,--seed", prod->gen.seed, "Random generator seed (default: taken from random device).");
  sub->add_flag("--pretty", prod->pretty, "Generate \"pretty-printed\" JSONs.");
  sub->add_flag("-v", prod->verbose, "Print the JSONs to stdout, even if -o or --output is used.");
  sub->add_option("-t,--threads", prod->num_threads, "Number of threads to use to generate JSONs (default=1).");
}

auto AppOptions::FromArguments(int argc, char *argv[], AppOptions *out) -> Status {
  AppOptions result;

  CLI::App app{std::string(AppOptions::name) + ": " + AppOptions::desc};

  std::string schema_file;
  uint16_t stream_port = 0;

  // File mode:
  auto *sub_file = app.add_subcommand("file", "Generate a file with JSONs.");
  AddCommonOpts(sub_file, &result.file.production, &schema_file);
  sub_file->add_option("-o,--output", result.file.out_path, "Output file. JSONs will be written to stdout if not set.");

  // Streaming server mode:
  auto *sub_stream = app.add_subcommand("stream", "Stream raw JSONs over a TCP network socket.");
  AddCommonOpts(sub_stream, &result.stream.production, &schema_file);
  auto *port_opt = sub_stream->add_option("-p,--port", stream_port, "Port (default=" + std::to_string(ZMQ_PORT) + ").");
  auto *zmq_flag = sub_stream->add_flag("-z,--zeromq", "Use the ZeroMQ push-pull protocol for the stream.");


  // Attempt to parse the CLI arguments.
  try {
    app.parse(argc, argv);
  } catch (CLI::CallForHelp &e) {
    // User wants to see help.
    std::cout << app.help() << std::endl;
    return Status::OK();
  } catch (CLI::Error &e) {
    return Status(Error::CLIError, e.get_name() + ": " + e.what() + "\n" + app.help());
  }

  // Handle subcommands. All of them require to load a serialized Arrow schema, so we can just return the
  // status of attempting to load that.
  // TODO(johanpel): push this down
  Status status;
  if (sub_file->parsed()) {
    result.sub = SubCommand::FILE;
    status = ReadSchemaFromFile(schema_file, &result.file.production.schema);
  } else if (sub_stream->parsed()) {
    result.sub = SubCommand::STREAM;
    status = ReadSchemaFromFile(schema_file, &result.stream.production.schema);

    // Check which streaming protocol to use.
    if (*zmq_flag) {
      ZMQProtocol zmq;
      if (*port_opt) {
        zmq.port = stream_port;
      }
      result.stream.protocol = zmq;
    } else {
      RawProtocol raw;
      if (*port_opt) {
        raw.port = stream_port;
      }
      result.stream.protocol = raw;
    }
  } else {
    result.sub = SubCommand::NONE;
  }

  *out = result;

  return status;
}

} // namespace illex
