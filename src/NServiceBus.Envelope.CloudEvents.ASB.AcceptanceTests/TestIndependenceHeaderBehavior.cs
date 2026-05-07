namespace NServiceBus.Envelope.CloudEvents.ASB.AcceptanceTests;

using AcceptanceTesting;
using Microsoft.Extensions.DependencyInjection;
using Pipeline;

class TestIndependenceHeaderBehavior : IBehavior<IOutgoingPhysicalMessageContext, IOutgoingPhysicalMessageContext>
{
    public Task Invoke(IOutgoingPhysicalMessageContext context, Func<IOutgoingPhysicalMessageContext, Task> next)
    {
        var scenarioContext = context.Builder.GetRequiredService<ScenarioContext>();
        context.Headers["$AcceptanceTesting.TestRunId"] = scenarioContext.TestRunId.ToString();
        return next(context);
    }
}