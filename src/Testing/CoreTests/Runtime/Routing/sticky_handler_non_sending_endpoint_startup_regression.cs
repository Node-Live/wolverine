using Microsoft.Extensions.Hosting;
using Wolverine.Configuration;
using Wolverine.Runtime;
using Wolverine.Transports;
using Wolverine.Transports.Sending;
using Xunit;

namespace CoreTests.Runtime.Routing;

// GH-4510: PrepopulateRoutingCache (new in 6.0, runs during StartAsync) crashes host startup
// for any message type sticky-bound (via AddStickyHandler) to a listen-only endpoint that
// cannot act as a sender -- e.g. an Azure Service Bus subscription, whose CreateSender
// unconditionally throws NotSupportedException (subscriptions have never supported sending).
// LocalRouting.FindRoutes / LocalTransport.DiscoverSenders treats every sticky-bound endpoint
// as a valid local-sending candidate and calls MessageRoute.For(...) on it unconditionally,
// which requires building a sender even though sticky binding is purely an INBOUND delivery
// assignment and says nothing about whether the endpoint can send.
//
// HandlerChain only assigns sticky endpoints when more than one handler exists for the message
// type (HandlerChain.cs: "if (grouping.Count() > 1) maybeAssignStickyHandlers(...)") -- exactly
// the real-world shape this bug was found in (several ASB subscriptions on one topic, all
// delivering the same message type to different sticky handlers, so Wolverine's default
// global-by-type dispatch can't tell them apart without sticky binding). Two handlers for the
// same message type, both sticky-bound to non-sending endpoints, reproduces it.
//
// This reproduces the crash with a minimal, self-contained fake transport (no ASB, no real
// broker) so it runs anywhere in CI without external dependencies.
public class sticky_handler_non_sending_endpoint_startup_regression
{
    [Fact]
    public async Task startup_does_not_crash_when_sticky_bound_endpoints_cannot_send()
    {
        using var host = await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Discovery.DisableConventionalDiscovery();
                opts.Discovery.IncludeType(typeof(NonSendingProbeHandlerOne));
                opts.Discovery.IncludeType(typeof(NonSendingProbeHandlerTwo));

                var transport = opts.Transports.GetOrCreate<NonSendingTransport>();

                var endpointOne = new NonSendingSubscriptionEndpoint(new Uri("nonsending://probe-subscription-one"));
                transport.Register(endpointOne);
                new ListenerConfiguration(endpointOne).AddStickyHandler(typeof(NonSendingProbeHandlerOne));

                var endpointTwo = new NonSendingSubscriptionEndpoint(new Uri("nonsending://probe-subscription-two"));
                transport.Register(endpointTwo);
                new ListenerConfiguration(endpointTwo).AddStickyHandler(typeof(NonSendingProbeHandlerTwo));
            })
            .StartAsync(TestContext.Current.CancellationToken);

        // Reaching here at all is the assertion. Pre-fix, StartAsync throws
        // NotSupportedException from PrepopulateRoutingCache -> LocalRouting.FindRoutes ->
        // MessageRoute.For -> Endpoint.StartSending -> NonSendingSubscriptionEndpoint.CreateSender,
        // before any handler ever runs.
    }
}

public sealed record NonSendingProbe;

public static class NonSendingProbeHandlerOne
{
    public static void Handle(NonSendingProbe message)
    {
        // Never actually invoked by this test -- only host startup is under test.
    }
}

public static class NonSendingProbeHandlerTwo
{
    public static void Handle(NonSendingProbe message)
    {
        // Never actually invoked by this test -- only host startup is under test.
    }
}

// A minimal transport, just enough to register one custom endpoint. TransportBase<TEndpoint>
// supplies everything else (GetOrCreateEndpoint, TryGetEndpoint, compilation) generically.
public sealed class NonSendingTransport() : TransportBase<NonSendingSubscriptionEndpoint>("nonsending", "Non-sending test transport", [])
{
    private readonly List<NonSendingSubscriptionEndpoint> _endpoints = [];

    public void Register(NonSendingSubscriptionEndpoint endpoint) => _endpoints.Add(endpoint);

    protected override IEnumerable<NonSendingSubscriptionEndpoint> endpoints() => _endpoints;

    protected override NonSendingSubscriptionEndpoint findEndpointByUri(Uri uri) =>
        _endpoints.Single(e => e.Uri == uri);
}

// Mirrors AzureServiceBusSubscription's shape exactly: a listen-only endpoint whose
// CreateSender unconditionally throws NotSupportedException, because the underlying broker
// concept (an ASB subscription) has never supported being sent to.
public sealed class NonSendingSubscriptionEndpoint(Uri uri)
    : Endpoint(uri, EndpointRole.Application), IListener, IChannelCallback
{
    public NonSendingSubscriptionEndpoint()
        : this(new Uri("nonsending://unnamed"))
    {
    }

    public override ValueTask<IListener> BuildListenerAsync(IWolverineRuntime runtime, IReceiver receiver)
    {
        IsListener = true;
        return new ValueTask<IListener>(this);
    }

    protected override ISender CreateSender(IWolverineRuntime runtime) =>
        throw new NotSupportedException("Subscriptions (and this fake) have never supported being a sender.");

    Uri IListener.Address => Uri;

    public ValueTask StopAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    IHandlerPipeline? IChannelCallback.Pipeline => null;

    public ValueTask CompleteAsync(Envelope envelope) => ValueTask.CompletedTask;

    public ValueTask DeferAsync(Envelope envelope) => ValueTask.CompletedTask;
}
