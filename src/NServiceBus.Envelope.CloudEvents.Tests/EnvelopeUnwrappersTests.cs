namespace NServiceBus.Envelope.CloudEvents.Tests;

using NUnit.Framework;

[TestFixture]
public class EnvelopeUnwrappersTests
{
    [Test]
    public void Should_return_null_if_not_found()
    {
        var unwrappers = new EnvelopeUnwrappers();
        var unwrapper = unwrappers.Find<CloudEventAmqpBinaryEnvelopeUnwrapper>();
        unwrappers.Remove(unwrapper!);

        var shouldBeNull = unwrappers.Find<CloudEventAmqpBinaryEnvelopeUnwrapper>();

        Assert.That(shouldBeNull, Is.Null);
    }
}