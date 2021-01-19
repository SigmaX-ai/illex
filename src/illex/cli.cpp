// Copyright 2020 Teratide B.V.
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
#include "illex/arrow.h"
#include "illex/status.h"

namespace illex {

/// \brief Common options for all subcommands
static void AddCommonOpts(CLI::App *sub,
                          ProductionOptions *prod,
                          std::string *schema_file) {
  sub->add_option("input,-i,--input",
                  *schema_file,
                  "An Arrow schema to generate the JSON from.")
      ->required()->check(CLI::ExistingFile);
  sub->add_option("-m,--num-jsons",
                  prod->num_jsons,
                  "Number of JSONs to send (default=1).");
  sub->add_option("-s,--seed",
                  prod->gen.seed,
                  "Random generator seed (default: taken from random device).");
  sub->add_flag("--pretty", prod->pretty, "Generate \"pretty-printed\" JSONs.");
  sub->add_flag("-v",
                prod->verbose,
                "Print the JSONs to stdout, even if -o or --output is used.");
  sub->add_option("-t,--threads",
                  prod->num_threads,
                  "Number of threads to use to generate JSONs (default=1).");
}

auto AppOptions::FromArguments(int argc, char *argv[], AppOptions *out) -> Status {
  AppOptions result;
  std::string schema_file;
  uint16_t stream_port = 0;

  CLI::App app{std::string(AppOptions::name) + ": " + AppOptions::desc};

  app.require_subcommand();

  // File mode:
  auto *file = app.add_subcommand("file", "Generate a file with JSONs.");
  AddCommonOpts(file, &result.file.production, &schema_file);
  file->add_option("-o,--output",
                   result.file.out_path,
                   "Output file. JSONs will be written to stdout if not set.");

  // Streaming server mode:
  auto *stream =
      app.add_subcommand("stream", "Stream raw JSONs over a TCP network socket.");
  AddCommonOpts(stream, &result.stream.production, &schema_file);
  auto *port_opt = stream->add_option("-p,--port",
                                      stream_port,
                                      "Port (default=" + std::to_string(RAW_PORT) + ").");
  auto *no_reuse_flag = stream->add_flag("--disable-socket-reuse",
                                         "Don't allow reuse of the server socket "
                                         "(need to wait for timeout).");
  auto *repeat = stream->add_flag("--repeat",
                                  "Indefinitely repeat creating the server and streaming "
                                  "the messages.");


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

  // Handle subcommands. All of them require to load a serialized Arrow schema, so we
  // can just return the status of attempting to load that.
  // TODO(johanpel): push this down
  Status status;
  if (file->parsed()) {
    result.sub = SubCommand::FILE;
    status = ReadSchemaFromFile(schema_file, &result.file.production.schema);
  } else if (stream->parsed()) {
    result.sub = SubCommand::STREAM;
    status = ReadSchemaFromFile(schema_file, &result.stream.production.schema);

    RawProtocol raw;
    if (*port_opt) raw.port = stream_port;
    if (*no_reuse_flag) raw.reuse = false;
    if (*repeat) result.stream.repeat = true;
    result.stream.protocol = raw;

  } else {
    result.sub = SubCommand::NONE;
  }

  *out = result;

  return status;
}

} // namespace illex