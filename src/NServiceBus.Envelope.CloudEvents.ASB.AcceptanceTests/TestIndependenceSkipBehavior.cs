namespace NServiceBus.Envelope.CloudEvents.ASB.AcceptanceTests;

using AcceptanceTesting;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Pipeline;

class TestIndependenceSkipBehavior : IBehavior<ITransportReceiveContext, ITransportReceiveContext>
{
    public Task Invoke(ITransportReceiveContext context, Func<ITransportReceiveContext, Task> next)
    {
        var scenarioContext = context.Builder.GetRequiredService<ScenarioContext>();
        var testRunId = scenarioContext.TestRunId.ToString();

        if (context.Message.Headers.TryGetValue("$AcceptanceTesting.TestRunId", out var runId) && runId != testRunId)
        {
            TestContext.Out.WriteLine($"Skipping message {context.Message.MessageId} from previous test run");
            return Task.CompletedTask;
        }

        return next(context);
    }
}