import { Commands } from "./src/commands.ts";
import { Denomander } from "./src/deps.ts";

const cli = new Denomander({
  app_name: "Mysql Queue CLI 📦",
});

const commands = Commands();
cli
  .command("queues")
  .description("list queues")
  .requiredOption("--uri", "db connection string")
  .action(() => {
    commands.listQueues(cli.uri);
  });

cli.parse(Deno.args);
