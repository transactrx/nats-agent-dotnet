using TransactRx.NatsAgent.Cli;

return await NatsAgentsCli.RunAsync(args, Console.In, Console.Out, Console.Error, Environment.GetEnvironmentVariable);
