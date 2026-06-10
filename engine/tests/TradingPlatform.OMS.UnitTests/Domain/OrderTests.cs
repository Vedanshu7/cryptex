using FluentAssertions;
using TradingPlatform.OMS.Domain.Entities;

namespace TradingPlatform.OMS.UnitTests.Domain;

public sealed class OrderTests
{
    [Fact]
    public void Create_SetsStatusToPending()
    {
        Order order = Order.Create(
            Guid.NewGuid(), "BTCUSDT", "BUY", 0.01m, "sig-1");

        order.Status.Should().Be(OrderStatus.Pending);
    }

    [Fact]
    public void Create_GeneratesUniqueId()
    {
        Order a = Order.Create(Guid.NewGuid(), "BTCUSDT", "BUY", 0.01m, null);
        Order b = Order.Create(Guid.NewGuid(), "BTCUSDT", "BUY", 0.01m, null);

        a.Id.Should().NotBe(b.Id);
    }

    [Fact]
    public void MarkValidated_TransitionsPendingToValidated()
    {
        Order order = Order.Create(Guid.NewGuid(), "BTCUSDT", "BUY", 0.01m, null);
        order.MarkValidated();

        order.Status.Should().Be(OrderStatus.Validated);
    }

    [Fact]
    public void MarkValidated_ThrowsWhenNotPending()
    {
        Order order = Order.Create(Guid.NewGuid(), "BTCUSDT", "BUY", 0.01m, null);
        order.MarkValidated();

        // Attempting to validate an already-validated order should throw.
        Action act = () => order.MarkValidated();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*Validated*");
    }

    [Fact]
    public void MarkFilled_SetsFilledPriceAndTimestamp()
    {
        Order order = Order.Create(Guid.NewGuid(), "BTCUSDT", "BUY", 0.01m, null);
        order.MarkValidated();
        order.MarkFilled(50_000m);

        order.Status.Should().Be(OrderStatus.Filled);
        order.Price.Should().Be(50_000m);
        order.FilledAt.Should().NotBeNull();
    }

    [Fact]
    public void MarkFilled_ThrowsWhenNotValidated()
    {
        Order order = Order.Create(Guid.NewGuid(), "BTCUSDT", "BUY", 0.01m, null);

        // Filling a PENDING order (not yet validated) should throw.
        Action act = () => order.MarkFilled(50_000m);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*Pending*");
    }

    [Fact]
    public void MarkRejected_SetsStatusToRejected()
    {
        Order order = Order.Create(Guid.NewGuid(), "BTCUSDT", "BUY", 0.01m, null);
        order.MarkRejected();

        order.Status.Should().Be(OrderStatus.Rejected);
    }

    [Fact]
    public void MarkCancelled_ThrowsOnTerminalStatus()
    {
        Order order = Order.Create(Guid.NewGuid(), "BTCUSDT", "BUY", 0.01m, null);
        order.MarkValidated();
        order.MarkFilled(50_000m);

        Action act = () => order.MarkCancelled();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*terminal*");
    }
}
